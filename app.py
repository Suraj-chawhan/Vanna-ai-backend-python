#!/usr/bin/env python3
"""
Full Flask backend for AI-powered analytics dashboard + chat-with-database.
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

# LangChain + Groq imports (optional AI SQL generator)
try:
    from langchain_community.utilities import SQLDatabase
    from langchain_groq import ChatGroq
    from langchain.chains import create_sql_query_chain
    from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
except Exception:
    SQLDatabase = ChatGroq = create_sql_query_chain = QuerySQLDataBaseTool = None

# -------------------------
# CONFIG
# -------------------------
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL environment variable must be set.")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__, static_folder=".", template_folder=".")

# -------------------------
# Database Helpers
# -------------------------
def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")

def to_json_serializable(v: Any) -> Any:
    import decimal
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v

# -------------------------
# LangChain / Groq Setup
# -------------------------
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
        logging.info("✅ LLM connected successfully (Groq + LangChain).")
    except Exception:
        logging.exception("Failed to initialize LLM components.")
else:
    logging.warning("⚠️ LLM not configured or GROQ_API_KEY missing.")

# -------------------------
# SQL safety utilities
# -------------------------
SQL_FENCE_RE = re.compile(r"```(?:sql|SQL)\s*(.*?)```", re.DOTALL)
DANGEROUS_SQL_KEYWORDS = [
    "insert ", "update ", "delete ", "drop ", "truncate ",
    "alter ", "create ", "grant ", "revoke ", "replace ",
    "backup ", "restore ", "copy ", "merge ", "execute "
]

def extract_sql(text: Optional[str]) -> str:
    """Extract SQL from model output."""
    if not text:
        return ""
    m = SQL_FENCE_RE.search(text)
    sql = m.group(1) if m else text
    sql = re.sub(r"(?mi)^\s*(question|sqlquery):\s*.*$", "", sql)
    return sql.strip("` \n\r\t")

def is_select_only(sql: str) -> bool:
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
    """Run safe SELECT queries."""
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

# -------------------------
# ROUTES
# -------------------------

@app.route("/")
def index():
    index_path = os.path.join(os.getcwd(), "index.html")
    if os.path.exists(index_path):
        return send_from_directory(".", "index.html")
    return jsonify({"message": "API Running — no frontend found."})

@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

# ---------- Dashboard Stats ----------
@app.route("/stats")
def stats():
    """Basic totals for dashboard."""
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT COUNT(*) AS total_invoices,
                   SUM(invoice_total) AS total_revenue,
                   AVG(invoice_total) AS avg_invoice
            FROM invoices;
        """)
        totals = cur.fetchone() or {}
        cur.execute("SELECT COUNT(*) AS total_customers FROM customers;")
        customers = cur.fetchone() or {}
        cur.execute("""
            SELECT v.vendor_name, SUM(i.invoice_total) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10;
        """)
        top_vendors = cur.fetchall()
        return jsonify({
            "stats_summary": {
                "total_invoices": int(totals.get("total_invoices") or 0),
                "total_revenue": float(totals.get("total_revenue") or 0),
                "avg_invoice": float(totals.get("avg_invoice") or 0),
                "total_customers": int(customers.get("total_customers") or 0)
            },
            "top_vendors": [
                {"vendor_name": r["vendor_name"], "total": float(r["total"])} for r in top_vendors
            ]
        })
    except Exception as e:
        logging.exception("/stats error")
        return jsonify({"error": str(e)}), 500

# ---------- Invoice Trends ----------
@app.route("/invoice-trends")
def invoice_trends():
    """Monthly invoice totals for chart."""
    try:
        sql = """
            SELECT DATE_TRUNC('month', invoice_date) AS month,
                   SUM(invoice_total) AS total
            FROM invoices
            GROUP BY month
            ORDER BY month;
        """
        data = run_select(sql)
        for r in data:
            r["month"] = r["month"][:10]
        return jsonify(data)
    except Exception as e:
        logging.exception("/invoice-trends error")
        return jsonify({"error": str(e)}), 500

# ---------- Top Vendors ----------
@app.route("/vendors/top10")
def vendors_top10():
    """Top 10 vendors by total invoice amount."""
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

# ---------- Cash Outflow ----------
@app.route("/cash-outflow")
def cash_outflow():
    """Recent payments."""
    try:
        sql = """
            SELECT p.payment_date, v.vendor_name, p.payment_total
            FROM payments p
            JOIN vendors v ON p.vendor_id = v.id
            ORDER BY p.payment_date DESC
            LIMIT 20;
        """
        return jsonify(run_select(sql))
    except Exception as e:
        logging.exception("/cash-outflow error")
        return jsonify({"error": str(e)}), 500

# ---------- AI SQL Chat ----------
@app.route("/ask", methods=["POST"])
def ask():
    if not use_llm:
        return jsonify({"error": "LLM not configured (set GROQ_API_KEY)."}), 400

    data = request.get_json(force=True)
    question = data.get("question")
    if not question:
        return jsonify({"error": "No question provided."}), 400

    try:
        raw_output = sql_generator.invoke({"question": question})
        sql_text = extract_sql(raw_output)
        logging.info(f"Generated SQL: {sql_text}")

        if not is_select_only(sql_text):
            return jsonify({"error": "Unsafe SQL generated.", "generated_sql": sql_text}), 400

        result = sql_runner_tool.invoke(sql_text)
        return jsonify({
            "question": question,
            "generated_sql": sql_text,
            "result": result
        })
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

# ---------- Run ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
