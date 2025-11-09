#!/usr/bin/env python3
import os
import re
import json
from datetime import datetime
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, send_from_directory

# LangChain / Groq LLM imports (for chat-with-data)
from langchain_community.utilities import SQLDatabase
from langchain_groq import ChatGroq
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool

# -------------------------
# CONFIG
# -------------------------
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://hunny:QTSFnZ96Zd7VdBTU6wqleJ8pXfPG97Ga@dpg-d47dbpili9vc738l3n00-a.singapore-postgres.render.com/flowbit"
)
GROQ_API_KEY = os.getenv("GROQ_API_KEY", None)  # set in env for production
LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-oss-20b")  # or "llama-3.3-70b-versatile"

# -------------------------
# APP + DB helpers
# -------------------------
app = Flask(__name__, static_folder=".", template_folder=".")

def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")

# -------------------------
# LLM Setup (for chat)
# -------------------------
use_llm = GROQ_API_KEY is not None
if use_llm:
    sql_db = SQLDatabase.from_uri(DB_URL)
    llm = ChatGroq(model=LLM_MODEL, groq_api_key=GROQ_API_KEY, temperature=0.05)
    sql_generator = create_sql_query_chain(llm, sql_db)
    sql_runner_tool = QuerySQLDataBaseTool(db=sql_db)
else:
    # placeholder
    sql_generator = None
    sql_runner_tool = None

# -------------------------
# SQL cleaning & safety
# -------------------------
SQL_FENCE_RE = re.compile(r"```sql(.*?)```", re.DOTALL | re.IGNORECASE)

def extract_sql(text: str) -> str:
    """Extract SQL from the LLM output (strip fences, question labels)."""
    if not text:
        return ""
    m = SQL_FENCE_RE.search(text)
    sql = m.group(1) if m else text
    # remove leading labels
    sql = re.sub(r'(?i)^question:.*$', '', sql, flags=re.MULTILINE).strip()
    sql = re.sub(r'(?i)^sqlquery:.*$', '', sql, flags=re.MULTILINE).strip()
    # remove markdown ticks left
    sql = sql.strip("` \n\r\t")
    return sql.strip()

def is_select_only(sql: str) -> bool:
    """Basic safety: only allow single SELECT queries (no semicolons chaining, no DML)."""
    if not sql:
        return False
    s = sql.strip().lower()
    # disallow dangerous keywords
    forbidden = ["insert ", "update ", "delete ", "drop ", "truncate ", "alter ", "create ", "grant ", "revoke ", "replace "]
    if any(k in s for k in forbidden):
        return False
    # disallow multiple statements
    if ";" in s and s.count(";") > 0:
        # allow trailing semicolon but not multiple statements
        s_no_semicolon = s.replace(";", "")
        # if more than one semicolon, reject
        if s.count(";") > 1:
            return False
    # must start with select or with (for CTE)
    return s.lstrip().startswith("select") or s.lstrip().startswith("with")

def run_select(sql: str):
    """Execute SELECT SQL and return rows as list of dicts."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        # Convert Decimal/Decimal objects to floats if present
        def fix(v):
            if isinstance(v, (int, float, str, bool)) or v is None:
                return v
            try:
                return float(v)
            except Exception:
                return v
        cleaned = [{k: fix(v) for k, v in row.items()} for row in rows]
        return cleaned
    except Exception as e:
        cur.close()
        conn.close()
        raise

# -------------------------
# ROUTES: dashboard data
# -------------------------

@app.route("/stats", methods=["GET"])
def route_stats():
    """
    /stats
    Returns:
      - top vendors (label,value)
      - totals: invoices count, revenue sum, avg invoice
      - total_customers
    """
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10
        """)
        top = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) AS total_invoices, COALESCE(SUM(invoice_total),0) AS total_revenue,
                   COALESCE(AVG(invoice_total),0) AS avg_invoice
            FROM invoices
        """)
        totals = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS total_customers FROM customers")
        customers = cur.fetchone()

        cur.close()
        conn.close()

        return jsonify({
            "top_vendors": top,
            "stats_summary": {
                "total_invoices": int(totals.get("total_invoices", 0)),
                "total_revenue": float(totals.get("total_revenue", 0)),
                "avg_invoice": float(totals.get("avg_invoice", 0)),
                "total_customers": int(customers.get("total_customers", 0))
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/invoice-trends", methods=["GET"])
def invoice_trends():
    """
    /invoice-trends
    Returns monthly invoice count and spend for the past 12 months.
    """
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
            ORDER BY 1 DESC
            LIMIT 12
        """)
        rows = cur.fetchall()
        # return in chronological order
        rows = list(reversed(rows))
        cur.close()
        conn.close()
        return jsonify({"monthly": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/vendors/top10", methods=["GET"])
def vendors_top10():
    """
    /vendors/top10
    Returns top 10 vendors by spend (label,value)
    """
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"vendors": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/category-spend", methods=["GET"])
def category_spend():
    """
    /category-spend
    Approximate spend grouped by 'category' inferred from line_items.description.
    If your original JSON had explicit categories, replace this with that column.
    Returns top categories and amounts.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Heuristic: first word/group of description as category; adjust to your data
        cur.execute("""
            SELECT
              COALESCE(NULLIF(split_part(li.description, ' ', 1), ''),'Uncategorized') AS category,
              COALESCE(SUM(li.total_price),0) AS total
            FROM line_items li
            JOIN invoices i ON li.invoice_id = i.id
            GROUP BY category
            ORDER BY total DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"categories": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cash-outflow", methods=["GET"])
def cash_outflow():
    """
    /cash-outflow
    Returns expected payments grouped by due_date buckets:
      - overdue, 0-7, 8-30, 31-90, 90+
    Uses payments.due_date if present; falls back to invoices.invoice_date + 30 days.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT
                CASE
                  WHEN (p.due_date IS NOT NULL AND p.due_date::date < current_date) THEN 'overdue'
                  WHEN (p.due_date IS NOT NULL AND p.due_date::date BETWEEN current_date AND current_date + interval '7 days') THEN '0-7'
                  WHEN (p.due_date IS NOT NULL AND p.due_date::date BETWEEN current_date + interval '8 days' AND current_date + interval '30 days') THEN '8-30'
                  WHEN (p.due_date IS NOT NULL AND p.due_date::date BETWEEN current_date + interval '31 days' AND current_date + interval '90 days') THEN '31-90'
                  ELSE '90+'
                END AS bucket,
                COALESCE(SUM(i.invoice_total),0) AS amount,
                COUNT(*) AS invoices_count
            FROM invoices i
            LEFT JOIN payments p ON p.invoice_id = i.id
            GROUP BY bucket
            ORDER BY bucket
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"buckets": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/invoices", methods=["GET"])
def invoices():
    """
    /invoices
    Query params:
      - vendor (partial match)
      - date_from (YYYY-MM-DD)
      - date_to (YYYY-MM-DD)
      - min_total, max_total
      - search (matches invoice_id or customer_name)
      - limit (default 100)
    """
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
            where_clauses.append("(i.invoice_id ILIKE %s OR c.customer_name ILIKE %s)")
            params.append(f"%{search}%")
            params.append(f"%{search}%")

        where_sql = " AND ".join(where_clauses)
        if where_sql:
            where_sql = "WHERE " + where_sql

        sql = f"""
            SELECT i.id, i.invoice_id, i.invoice_date, i.invoice_total, v.vendor_name, c.customer_name
            FROM invoices i
            LEFT JOIN vendors v ON i.vendor_id = v.id
            LEFT JOIN customers c ON i.customer_id = c.id
            {where_sql}
            ORDER BY i.invoice_date DESC NULLS LAST
            LIMIT %s
        """
        params.append(limit)

        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"invoices": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------
# Chat with LLM -> SQL -> results (safe)
# -------------------------
@app.route("/chat-with-data", methods=["POST"])
def chat_with_data():
    """
    POST /chat-with-data { "question": "Which vendor generated the highest total invoice amount?" }
    Uses the LLM to generate SQL, extracts the SQL, ensures it's SELECT-only, executes and returns JSON.
    """
    if not use_llm:
        return jsonify({"error": "LLM not configured. Set GROQ_API_KEY in environment."}), 400

    payload = request.get_json(force=True)
    question = payload.get("question")
    if not question:
        return jsonify({"error": "No question provided."}), 400

    try:
        # Generate SQL (LangChain chain returns the SQL text)
        raw_output = sql_generator.invoke({"question": question})
        sql_text = extract_sql(raw_output)

        if not is_select_only(sql_text):
            return jsonify({"error": "LLM produced non-SELECT or unsafe SQL. Aborting.", "generated_sql": sql_text}), 400

        # Execute the SQL and return results
        result = sql_runner_tool.invoke(sql_text)
        return jsonify({"question": question, "generated_sql": sql_text, "result": result})
    except Exception as e:
        # include raw_output for debugging if available
        return jsonify({"error": str(e)}), 500

# -------------------------
# Serve root HTML
# -------------------------
@app.route("/", methods=["GET"])
def index():
    return send_from_directory(".", "index.html")

# -------------------------
# Health
# -------------------------
@app.route("/health", methods=["GET"])
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
