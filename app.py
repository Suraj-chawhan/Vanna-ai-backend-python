#!/usr/bin/env python3
"""
Flask backend for DataWand.
Routes:
 - /stats
 - /invoice-trends
 - /vendors/top10
 - /cash-outflow
 - /invoices
 - /ask   (optional; enabled when GROQ_API_KEY + required packages are installed)
"""
import os
import re
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, send_from_directory

# Optional LLM imports (only used for /ask)
try:
    from langchain_community.utilities import SQLDatabase
    from langchain_groq import ChatGroq
    from langchain.chains import create_sql_query_chain
    from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
except Exception:
    SQLDatabase = ChatGroq = create_sql_query_chain = QuerySQLDataBaseTool = None

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL environment variable must be set (postgres://...)")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

app = Flask(__name__, static_folder=".", template_folder=".")

def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")

def to_json_serializable(v: Any) -> Any:
    import decimal
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v

def run_select(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return [{k: to_json_serializable(v) for k, v in row.items()} for row in rows]
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# LLM setup (optional)
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
        logging.info("LLM configured (Groq).")
    except Exception:
        logging.exception("Failed to initialize LLM. /ask will be disabled.")

SQL_FENCE_RE = re.compile(r"```(?:sql|SQL)?\s*(.*?)```", re.DOTALL)
DANGEROUS_SQL_KEYWORDS = [
    "insert ", "update ", "delete ", "drop ", "truncate ", "alter ",
    "create ", "grant ", "revoke ", "replace ", "backup ", "restore ",
    "copy ", "merge ", "execute "
]

def extract_sql(text: Optional[str]) -> str:
    if not text:
        return ""
    m = SQL_FENCE_RE.search(text)
    sql = m.group(1) if m else text
    sql = re.sub(r'(?mi)^\s*(question|sqlquery):\s*.*$', '', sql)
    return sql.strip("` \n\r\t")

def is_select_only(sql: str) -> bool:
    if not sql:
        return False
    s = sql.strip().lower()
    if ";" in s:
        if s.count(";") > 1 or not s.endswith(";"):
            return False
        s = s.rstrip(";").strip()
    for bad in DANGEROUS_SQL_KEYWORDS:
        if bad in s:
            return False
    return s.startswith("select") or s.startswith("with")

@app.route("/")
def index():
    if os.path.exists("index.html"):
        return send_from_directory(".", "index.html")
    return jsonify({"message": "API running. Place index.html in project root to serve UI."})

@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        logging.exception("Health check failed")
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/stats")
def stats():
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT COUNT(*) AS total_invoices,
                   COALESCE(SUM(invoice_total),0) AS total_revenue,
                   COALESCE(AVG(invoice_total),0) AS avg_invoice
            FROM invoices;
        """)
        summary = cur.fetchone() or {}

        cur.execute("SELECT COUNT(*) AS total_customers FROM customers;")
        customers = cur.fetchone() or {}

        cur.execute("""
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
            FROM invoices i
            JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name
            ORDER BY total DESC
            LIMIT 10;
        """)
        vendors = cur.fetchall() or []

        return jsonify({
            "stats_summary": {
                "total_invoices": int(summary.get("total_invoices", 0)),
                "total_revenue": float(summary.get("total_revenue", 0.0)),
                "avg_invoice": float(summary.get("avg_invoice", 0.0)),
                "total_customers": int(customers.get("total_customers", 0))
            },
            "top_vendors": [{"vendor_name": v["vendor_name"], "total": float(v["total"])} for v in vendors]
        })
    except Exception as e:
        logging.exception("/stats error")
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route("/invoice-trends")
def invoice_trends():
    try:
        sql = """
            SELECT TO_CHAR(DATE_TRUNC('month', invoice_date), 'YYYY-MM') AS month,
                   COALESCE(SUM(invoice_total),0) AS total
            FROM invoices
            WHERE invoice_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1;
        """
        rows = run_select(sql)
        return jsonify({"monthly": rows})
    except Exception as e:
        logging.exception("/invoice-trends error")
        return jsonify({"error": str(e)}), 500

@app.route("/vendors/top10")
def vendors_top10():
    try:
        sql = """
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
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

@app.route("/cash-outflow")
def cash_outflow():
    try:
        sql = """
            SELECT p.payment_date, v.vendor_name, p.payment_total
            FROM payments p
            JOIN vendors v ON p.vendor_id = v.id
            WHERE p.payment_total IS NOT NULL
            ORDER BY p.payment_date DESC
            LIMIT 25;
        """
        rows = run_select(sql)
        return jsonify({"rows": rows})
    except Exception as e:
        logging.exception("/cash-outflow error")
        return jsonify({"error": str(e)}), 500

@app.route("/invoices")
def invoices():
    try:
        vendor = request.args.get("vendor")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        min_total = request.args.get("min_total")
        max_total = request.args.get("max_total")
        search = request.args.get("search")
        limit = int(request.args.get("limit", 100))

        where = []
        params = []

        if vendor:
            where.append("v.vendor_name ILIKE %s")
            params.append(f"%{vendor}%")
        if date_from:
            where.append("i.invoice_date >= %s")
            params.append(date_from)
        if date_to:
            where.append("i.invoice_date <= %s")
            params.append(date_to)
        if min_total:
            where.append("i.invoice_total >= %s")
            params.append(min_total)
        if max_total:
            where.append("i.invoice_total <= %s")
            params.append(max_total)
        if search:
            where.append("(i.invoice_id::text ILIKE %s OR c.customer_name ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
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
        rows = run_select(sql, tuple(params))
        return jsonify({"invoices": rows})
    except Exception as e:
        logging.exception("/invoices error")
        return jsonify({"error": str(e)}), 500

@app.route("/ask", methods=["POST"])
def ask():
    if not use_llm:
        return jsonify({"error": "LLM not configured. Set GROQ_API_KEY and install dependencies."}), 400
    payload = request.get_json(force=True)
    question = payload.get("question")
    if not question:
        return jsonify({"error": "No question provided."}), 400
    try:
        raw_output = sql_generator.invoke({"question": question})
        sql_text = extract_sql(raw_output)
        logging.info("Generated SQL: %s", sql_text)
        if not is_select_only(sql_text):
            return jsonify({"error": "Unsafe SQL.", "generated_sql": sql_text}), 400
        result = sql_runner_tool.invoke(sql_text)
        return jsonify({"question": question, "generated_sql": sql_text, "result": result})
    except Exception as e:
        logging.exception("/ask error")
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
     
