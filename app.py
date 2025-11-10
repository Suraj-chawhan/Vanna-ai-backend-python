#!/usr/bin/env python3
"""
Flask backend for AI-powered analytics dashboard.
- PostgreSQL for data
- Groq LLM for SQL generation
"""

import os
import re
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, send_from_directory

# -----------------------------
# Optional LangChain + Groq imports
# -----------------------------
try:
    from langchain_community.utilities import SQLDatabase
    from langchain_groq import ChatGroq
    from langchain.chains import create_sql_query_chain
    from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
except Exception:
    SQLDatabase = ChatGroq = create_sql_query_chain = QuerySQLDataBaseTool = None

# -----------------------------
# CONFIGURATION
# -----------------------------
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL environment variable must be set (e.g. postgres://...).")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__, static_folder=".", template_folder=".")

# -----------------------------
# DATABASE CONNECTION
# -----------------------------
def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")

def to_json_serializable(v: Any) -> Any:
    import decimal
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v

# -----------------------------
# LLM SETUP (optional)
# -----------------------------
use_llm = False
sql_generator = None
sql_runner_tool = None

if GROQ_API_KEY and SQLDatabase and ChatGroq:
    try:
        sql_db = SQLDatabase.from_uri(DB_URL)
        llm = ChatGroq(model=LLM_MODEL, groq_api_key=GROQ_API_KEY, temperature=0.05)
        sql_generator = create_sql_query_chain(llm, sql_db)
        sql_runner_tool = QuerySQLDataBaseTool(db=sql_db)
        use_llm = True
        logging.info("✅ LLM configured (Groq connected).")
    except Exception as e:
        logging.exception("Failed to initialize LLM; continuing without it.")
else:
    logging.warning("⚠️ Groq not configured or missing packages.")

# -----------------------------
# SQL SAFETY HELPERS
# -----------------------------
SQL_FENCE_RE = re.compile(r"```(?:sql|SQL)\s*(.*?)```", re.DOTALL)
DANGEROUS_SQL_KEYWORDS = [
    "insert ", "update ", "delete ", "drop ", "truncate ", "alter ",
    "create ", "grant ", "revoke ", "replace ", "backup ", "restore ",
    "copy ", "merge ", "execute "
]

def extract_sql(text: Optional[str]) -> str:
    """Extract SQL from LLM output safely."""
    if not text:
        return ""
    m = SQL_FENCE_RE.search(text)
    sql = m.group(1) if m else text
    sql = re.sub(r"(?mi)^\s*(question|sqlquery):\s*.*$", "", sql)
    return sql.strip("` \n\r\t")

def is_select_only(sql: str) -> bool:
    """Allow only SELECT/WITH queries (safe read-only)."""
    if not sql:
        return False
    s = sql.strip().lower()
    if ";" in s and (s.count(";") > 1 or not s.endswith(";")):
        return False
    for bad in DANGEROUS_SQL_KEYWORDS:
        if bad in s:
            return False
    return s.startswith("select") or s.startswith("with")

def run_select(sql: str) -> List[Dict[str, Any]]:
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        return [{k: to_json_serializable(v) for k, v in row.items()} for row in rows]
    finally:
        if cur: cur.close()
        if conn: conn.close()

# -----------------------------
# ROUTES
# -----------------------------

@app.route("/")
def index():
    """Serve frontend."""
    if os.path.exists("index.html"):
        return send_from_directory(".", "index.html")
    return jsonify({"message": "API is running. Upload index.html for UI."})

@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# ---------- /stats ----------
@app.route("/stats")
def stats():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Summary
        cur.execute("""
            SELECT COUNT(*) AS total_invoices,
                   COALESCE(SUM(invoice_total), 0) AS total_revenue,
                   COALESCE(AVG(invoice_total), 0) AS avg_invoice
            FROM invoices;
        """)
        summary = cur.fetchone() or {}

        # Customers
        cur.execute("SELECT COUNT(*) AS total_customers FROM customers;")
        customers = cur.fetchone() or {}

        # Top Vendors
        cur.execute("""
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10;
        """)
        vendors = cur.fetchall()

        return jsonify({
            "stats_summary": {
                "total_invoices": int(summary.get("total_invoices", 0)),
                "total_revenue": float(summary.get("total_revenue", 0.0)),
                "avg_invoice": float(summary.get("avg_invoice", 0.0)),
                "total_customers": int(customers.get("total_customers", 0))
            },
            "top_vendors": [
                {"vendor_name": v["vendor_name"], "total": float(v["total"])} for v in vendors
            ]
        })

    except Exception as e:
        logging.exception("/stats error")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

# ---------- /invoice-trends ----------
@app.route("/invoice-trends")
def invoice_trends():
    try:
        sql = """
            SELECT TO_CHAR(DATE_TRUNC('month', invoice_date), 'YYYY-MM') AS month,
                   SUM(invoice_total) AS total
            FROM invoices
            WHERE invoice_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1;
        """
        return jsonify(run_select(sql))
    except Exception as e:
        logging.exception("/invoice-trends error")
        return jsonify({"error": str(e)}), 500

# ---------- /vendors/top10 ----------
@app.route("/vendors/top10")
def vendors_top10():
    try:
        sql = """
            SELECT v.vendor_name, SUM(i.invoice_total) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10;
        """
        return jsonify(run_select(sql))
    except Exception as e:
        logging.exception("/vendors/top10 error")
        return jsonify({"error": str(e)}), 500

# ---------- /cash-outflow ----------
@app.route("/cash-outflow")
def cash_outflow():
    """
    Returns recent payments with vendor breakdown.
    Used for recent cash outflow dashboard + pie chart.
    """
    try:
        sql = """
            SELECT 
                p.payment_date,
                v.vendor_name,
                p.payment_total
            FROM payments p
            JOIN vendors v ON p.vendor_id = v.id
            ORDER BY p.payment_date DESC
            LIMIT 25;
        """
        rows = run_select(sql)
        return jsonify({"rows": rows})
    except Exception as e:
        logging.exception("/cash-outflow error")
        return jsonify({"error": str(e)}), 500



# ---------- /ask (Groq Chat-to-SQL) ----------
@app.route("/ask", methods=["POST"])
def ask():
    if not use_llm:
        return jsonify({"error": "LLM not configured. Set GROQ_API_KEY."}), 400

    payload = request.get_json(force=True)
    question = payload.get("question")
    if not question:
        return jsonify({"error": "No question provided."}), 400

    try:
        raw_output = sql_generator.invoke({"question": question})
        sql_text = extract_sql(raw_output)

        logging.info(f"🧠 Generated SQL: {sql_text}")

        if not is_select_only(sql_text):
            return jsonify({"error": "Unsafe or invalid SQL generated.", "generated_sql": sql_text}), 400

        result = sql_runner_tool.invoke(sql_text)
        return jsonify({"question": question, "generated_sql": sql_text, "result": result})

    except Exception as e:
        logging.exception("/ask error")
        return jsonify({"error": str(e)}), 500

# ---------- Error Handlers ----------
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error"}), 500

# ---------- MAIN ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
        
