#!/usr/bin/env python3
"""
Flask app for invoices analytics + "chat-with-data" SQL generator runner.

Environment:
  - DB_URL: postgres connection string (required)
  - GROQ_API_KEY: optional, if set enables ChatGroq-based SQL generation
  - LLM_MODEL: optional model id for ChatGroq (default: openai/gpt-oss-20b)
"""
import os
import re
import json
import logging
from datetime import datetime
from typing import Optional, Any, Dict, List

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, send_from_directory

# LangChain / Groq LLM imports (for chat-with-data)
# NOTE: these imports will only work if you installed the appropriate packages.
# If you don't want LLM support, don't set GROQ_API_KEY in the environment.
try:
    from langchain_community.utilities import SQLDatabase
    from langchain_groq import ChatGroq
    from langchain.chains import create_sql_query_chain
    from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
except Exception:
    # Keep application working even if LLM deps are not installed.
    SQLDatabase = None
    ChatGroq = None
    create_sql_query_chain = None
    QuerySQLDataBaseTool = None

# -------------------------
# CONFIG
# -------------------------
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL environment variable must be set to your Postgres URL.")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", None)
LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-oss-20b")

# Basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------------
# APP + DB helpers
# -------------------------
app = Flask(__name__, static_folder=".", template_folder=".")

def get_conn():
    """
    Return a new psycopg2 connection using DB_URL. Caller must close connection.
    """
    return psycopg2.connect(DB_URL, sslmode="require")

def to_json_serializable(value: Any) -> Any:
    """
    Convert certain DB types to JSON-serializable Python types.
    """
    # psycopg2 usually returns Python-native types already (int, float, str, datetime, Decimal)
    # Convert datetime -> ISO string, Decimal -> float if safe
    try:
        import decimal
        if isinstance(value, decimal.Decimal):
            return float(value)
    except Exception:
        pass

    if isinstance(value, datetime):
        return value.isoformat()
    # Other types (int, float, str, bool, None) returned as-is
    return value

# -------------------------
# LLM Setup (optional)
# -------------------------
use_llm = False
sql_generator = None
sql_runner_tool = None

if GROQ_API_KEY and SQLDatabase and ChatGroq and create_sql_query_chain and QuerySQLDataBaseTool:
    try:
        sql_db = SQLDatabase.from_uri(DB_URL)
        llm = ChatGroq(model=LLM_MODEL, groq_api_key=GROQ_API_KEY, temperature=0.05)
        sql_generator = create_sql_query_chain(llm, sql_db)
        sql_runner_tool = QuerySQLDataBaseTool(db=sql_db)
        use_llm = True
        logging.info("LLM configured (ChatGroq).")
    except Exception as e:
        logging.exception("Failed to initialize LLM components; continuing without LLM support.")
        use_llm = False
else:
    if GROQ_API_KEY:
        logging.warning("GROQ_API_KEY is set but LLM packages are not installed/available. LLM disabled.")
    else:
        logging.info("LLM not configured (GROQ_API_KEY not provided).")

# -------------------------
# SQL utilities
# -------------------------
# Match ```sql ... ``` or ```SQL ... ``` (non-greedy)
SQL_FENCE_RE = re.compile(r"```(?:sql|SQL)\s*(.*?)```", re.DOTALL)

def extract_sql(text: Optional[str]) -> str:
    """
    Extract SQL from a text blob. Handles fenced code blocks like ```sql ... ``` and
    tries to remove common prefixes like "question:" or "sqlquery:".

    Returns the extracted SQL (trimmed) or empty string when nothing found.
    """
    if not text:
        return ""
    # Some LLMs return JSON-like or markup text. If the response contains HTML tags
    # (like "<div>"), we'll try to strip common HTML tags to reduce "unexpected <" problems.
    # But we do not attempt to sanitize arbitrary HTML — we just aim to avoid a simple issue.
    # If you expect HTML, handle it upstream.
    # Quick removal of top-level HTML tags that wrap content:
    if "<html" in text.lower() or "<div" in text.lower() or "<p" in text.lower():
        # Remove simple tags but keep inner text
        text = re.sub(r"<\/?pre>|<\/?code>|<\/?div>|<\/?p>|<\/?span>|<\/?html>|<\/?body>|<\/?head>", "", text, flags=re.IGNORECASE)

    m = SQL_FENCE_RE.search(text)
    sql = m.group(1) if m else text

    # Remove leading 'question:' or 'sqlquery:' lines if present (case-insensitive)
    sql = re.sub(r'(?mi)^\s*question:\s*.*$', '', sql).strip()
    sql = re.sub(r'(?mi)^\s*sqlquery:\s*.*$', '', sql).strip()

    # Remove any triple backticks / leftover code fences and leading/trailing backticks/spaces
    sql = sql.strip("` \n\r\t")
    return sql.strip()

# Disallow dangerous SQL keywords and multiple statements
DANGEROUS_SQL_KEYWORDS = [
    "insert ", "update ", "delete ", "drop ", "truncate ", "alter ", "create ", "grant ", "revoke ",
    "replace ", "backup ", "restore ", "copy ", "merge ", "execute "
]

def is_select_only(sql: str) -> bool:
    """
    Basic guard to allow only SELECT/WITH queries. Returns True if it looks safe to run.
    This is conservative, not perfect. Do not use for highly sensitive deployments.
    """
    if not sql:
        return False
    s = sql.strip().lower()

    # Block multiple statements (presence of semicolon followed by non-space indicates multiple)
    # Allow a single trailing semicolon optionally.
    if ";" in s:
        # If there is more than one semicolon, reject
        if s.count(";") > 1:
            return False
        # If semicolon exists but there is text after it, reject
        if s.rfind(";") != len(s) - 1:
            return False
        # strip trailing semicolon for next checks
        s = s.rstrip(";").strip()

    # Block dangerous keywords anywhere in the statement
    for bad in DANGEROUS_SQL_KEYWORDS:
        if bad in s:
            return False

    # Accept only statements that start with 'select' or 'with'
    return s.startswith("select") or s.startswith("with")

def run_select(sql: str) -> List[Dict[str, Any]]:
    """
    Execute a SELECT-style SQL and return a list of JSON-serializable rows.
    Raises Exception on errors.
    """
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        # Convert types for JSON
        results = []
        for row in rows:
            converted = {}
            for k, v in row.items():
                converted[k] = to_json_serializable(v)
            results.append(converted)
        return results
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# -------------------------
# ROUTES
# -------------------------

@app.route("/stats")
def route_stats():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10;
        """)
        top = [dict((k, to_json_serializable(v)) for k, v in row.items()) for row in cur.fetchall()]

        cur.execute("""
            SELECT COUNT(*) AS total_invoices, COALESCE(SUM(invoice_total),0) AS total_revenue,
                   COALESCE(AVG(invoice_total),0) AS avg_invoice
            FROM invoices;
        """)
        totals = cur.fetchone() or {}

        cur.execute("SELECT COUNT(*) AS total_customers FROM customers;")
        customers = cur.fetchone() or {}

        return jsonify({
            "top_vendors": top,
            "stats_summary": {
                "total_invoices": int(totals.get("total_invoices") or 0),
                "total_revenue": float(totals.get("total_revenue") or 0.0),
                "avg_invoice": float(totals.get("avg_invoice") or 0.0),
                "total_customers": int(customers.get("total_customers") or 0)
            }
        })
    except Exception as e:
        logging.exception("Error in /stats")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


@app.route("/invoice-trends")
def invoice_trends():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT
                to_char(date_trunc('month', COALESCE(invoice_date, CURRENT_DATE)),'YYYY-MM') AS month,
                COUNT(*) AS invoice_count,
                COALESCE(SUM(invoice_total),0) AS total_spend
            FROM invoices
            WHERE invoice_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1 ASC
            LIMIT 12;
        """)
        rows = [dict((k, to_json_serializable(v)) for k, v in row.items()) for row in cur.fetchall()]
        return jsonify({"monthly": rows})
    except Exception as e:
        logging.exception("Error in /invoice-trends")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


@app.route("/vendors/top10")
def vendors_top10():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10;
        """)
        rows = [dict((k, to_json_serializable(v)) for k, v in row.items()) for row in cur.fetchall()]
        return jsonify({"vendors": rows})
    except Exception as e:
        logging.exception("Error in /vendors/top10")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


@app.route("/category-spend")
def category_spend():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT
                COALESCE(NULLIF(split_part(li.description, ' ', 1), ''),'Uncategorized') AS category,
                COALESCE(SUM(li.total_price),0) AS total
            FROM line_items li
            JOIN invoices i ON li.invoice_id = i.id
            GROUP BY category
            ORDER BY total DESC
            LIMIT 10;
        """)
        rows = [dict((k, to_json_serializable(v)) for k, v in row.items()) for row in cur.fetchall()]
        return jsonify({"categories": rows})
    except Exception as e:
        logging.exception("Error in /category-spend")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


@app.route("/cash-outflow")
def cash_outflow():
    """
    Returns forecast of upcoming cash outflows based on due dates.
    Buckets invoices into time ranges.
    """
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT
                CASE
                    WHEN COALESCE(
                        NULLIF(p.due_date, ''), 
                        (i.invoice_date + INTERVAL '30 days')::text
                    )::date < CURRENT_DATE THEN 'Overdue'
                    WHEN COALESCE(
                        NULLIF(p.due_date, ''), 
                        (i.invoice_date + INTERVAL '30 days')::text
                    )::date BETWEEN CURRENT_DATE AND (CURRENT_DATE + INTERVAL '7 days')
                        THEN '0-7 Days'
                    WHEN COALESCE(
                        NULLIF(p.due_date, ''), 
                        (i.invoice_date + INTERVAL '30 days')::text
                    )::date BETWEEN (CURRENT_DATE + INTERVAL '8 days') AND (CURRENT_DATE + INTERVAL '30 days')
                        THEN '8-30 Days'
                    WHEN COALESCE(
                        NULLIF(p.due_date, ''), 
                        (i.invoice_date + INTERVAL '30 days')::text
                    )::date BETWEEN (CURRENT_DATE + INTERVAL '31 days') AND (CURRENT_DATE + INTERVAL '90 days')
                        THEN '31-90 Days'
                    ELSE '90+ Days'
                END AS bucket,
                COALESCE(SUM(i.invoice_total), 0) AS amount,
                COUNT(*) AS invoice_count
            FROM invoices i
            LEFT JOIN payments p ON p.invoice_id = i.id
            GROUP BY 1
            ORDER BY 1;
        """

        cur.execute(query)
        rows = [dict((k, to_json_serializable(v)) for k, v in row.items()) for row in cur.fetchall()]
        return jsonify({"buckets": rows})
    except Exception as e:
        logging.exception("Error in /cash-outflow")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass

@app.route("/invoices")
def invoices():
    conn = None
    cur = None
    try:
        vendor = request.args.get("vendor")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        min_total = request.args.get("min_total")
        max_total = request.args.get("max_total")
        search = request.args.get("search")
        limit = int(request.args.get("limit", 100))

        where_clauses = []
        params = []

        if vendor:
            where_clauses.append("v.vendor_name ILIKE %s")
            params.append(f"%{vendor}%")
        if date_from:
            where_clauses.append("i.invoice_date >= %s")
            params.append(date_from)
        if date_to:
            where_clauses.append("i.invoice_date <= %s")
            params.append(date_to)
        if min_total:
            where_clauses.append("i.invoice_total >= %s")
            params.append(min_total)
        if max_total:
            where_clauses.append("i.invoice_total <= %s")
            params.append(max_total)
        if search:
            where_clauses.append("(i.invoice_id::text ILIKE %s OR c.customer_name ILIKE %s)")
            params += [f"%{search}%", f"%{search}%"]

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT i.id, i.invoice_id, i.invoice_date, i.invoice_total, v.vendor_name, c.customer_name
            FROM invoices i
            LEFT JOIN vendors v ON i.vendor_id = v.id
            LEFT JOIN customers c ON i.customer_id = c.id
            {where_sql}
            ORDER BY i.invoice_date DESC NULLS LAST
            LIMIT %s;
        """
        params.append(limit)

        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, tuple(params))
        rows = [dict((k, to_json_serializable(v)) for k, v in row.items()) for row in cur.fetchall()]
        return jsonify({"invoices": rows})
    except Exception as e:
        logging.exception("Error in /invoices")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur:
            try: cur.close()
            except Exception: pass
        if conn:
            try: conn.close()
            except Exception: pass


@app.route("/chat-with-data", methods=["POST"])
def chat_with_data():
    if not use_llm:
        return jsonify({"error": "LLM not configured. Set GROQ_API_KEY."}), 400

    payload = request.get_json(force=True)
    question = payload.get("question")
    if not question:
        return jsonify({"error": "No question provided."}), 400

    try:
        raw_output = sql_generator.invoke({"question": question})
        sql_text = extract_sql(raw_output)

        print("🧠 Generated SQL:", sql_text)

        if not is_select_only(sql_text):
            return jsonify({"error": "Unsafe SQL.", "generated_sql": sql_text}), 400

        result = sql_runner_tool.invoke(sql_text)
        print("✅ Query Result:", result)

        return jsonify({
            "question": question,
            "generated_sql": sql_text,
            "result": result
        })

    except Exception as e:
        print("❌ Chat-with-data error:", e)
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    # serve index if present; otherwise return a helpful message
    index_path = os.path.join(os.getcwd(), "index.html")
    if os.path.exists(index_path):
        return send_from_directory(".", "index.html")
    return jsonify({"message": "Index not present. API is running."})


@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        logging.exception("Health check failed")
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    # For local debugging; in production use gunicorn/uvicorn etc.
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
