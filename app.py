#!/usr/bin/env python3
import os
import re
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

try:
    from langchain_community.utilities import SQLDatabase
    from langchain_groq import ChatGroq
    from langchain.chains import create_sql_query_chain
    from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
except Exception:
    SQLDatabase = ChatGroq = create_sql_query_chain = QuerySQLDataBaseTool = None

DB_URL = os.getenv("DB_URL") or "postgresql://hunny:QTSFnZ96Zd7VdBTU6wqleJ8pXfPG97Ga@dpg-d47dbpili9vc738l3n00-a.singapore-postgres.render.com/flowbit"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "llama-3.3-70b-versatile")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
app = Flask(__name__, static_folder=".", template_folder=".")
CORS(app, resources={r"/*": {"origins": "*"}})

def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")

def to_json_serializable(v):
    import decimal
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v

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
        logging.info("✅ LLM connected (Groq + LangChain).")
    except Exception as e:
        logging.exception("LLM init failed.")
else:
    logging.warning("⚠️ LLM not configured. /ask will be disabled.")

SQL_FENCE_RE = re.compile(r"```(?:sql|SQL)\s*(.*?)```", re.DOTALL)
DANGEROUS = ["insert ", "update ", "delete ", "drop ", "truncate ", "alter ",
             "create ", "grant ", "revoke ", "replace ", "backup ",
             "restore ", "copy ", "merge ", "execute "]

def extract_sql(text: Optional[str]) -> str:
    if not text:
        return ""
    if "<html" in text.lower():
        text = re.sub(r"<.*?>", "", text)
    m = SQL_FENCE_RE.search(text)
    sql = m.group(1) if m else text
    sql = re.sub(r"(?mi)^\s*(question|sqlquery):\s*.*$", "", sql)
    return sql.strip("` \n\r\t")

def is_safe_select(sql: str) -> bool:
    s = sql.strip().lower()
    if any(x in s for x in DANGEROUS):
        return False
    if ";" in s and (s.count(";") > 1 or not s.endswith(";")):
        return False
    return s.startswith("select") or s.startswith("with")

def run_query(sql: str) -> List[Dict[str, Any]]:
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        return [{k: to_json_serializable(v) for k, v in r.items()} for r in rows]
    finally:
        if cur: cur.close()
        if conn: conn.close()

@app.route("/")
def index():
    if os.path.exists("index.html"):
        return send_from_directory(".", "index.html")
    return jsonify({"message": "API Running. Place index.html for UI."})

@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/stats")
def stats():
    conn = cur = None
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT COUNT(*) AS total_invoices, COALESCE(SUM(invoice_total),0) AS total_revenue,
                   COALESCE(AVG(invoice_total),0) AS avg_invoice FROM invoices;
        """)
        summary = cur.fetchone() or {}
        cur.execute("SELECT COUNT(*) AS total_customers FROM customers;")
        customers = cur.fetchone() or {}
        cur.execute("""
            SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
            FROM invoices i JOIN vendors v ON i.vendor_id = v.id
            GROUP BY v.vendor_name ORDER BY total DESC LIMIT 10;
        """)
        vendors = cur.fetchall()
        return jsonify({
            "stats_summary": {
                "total_invoices": int(summary.get("total_invoices", 0)),
                "total_revenue": float(summary.get("total_revenue", 0.0)),
                "avg_invoice": float(summary.get("avg_invoice", 0.0)),
                "total_customers": int(customers.get("total_customers", 0))
            },
            "top_vendors": vendors
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
                   SUM(invoice_total) AS total
            FROM invoices
            WHERE invoice_date IS NOT NULL
            GROUP BY 1 ORDER BY 1;
        """
        return jsonify({"monthly": run_query(sql)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        return jsonify(run_query(sql))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cash-outflow")
def cash_outflow():
    try:
        sql = """
            SELECT p.payment_date, v.vendor_name, COALESCE(p.payment_total,0) AS payment_total
            FROM payments p
            JOIN invoices i ON p.invoice_id = i.id
            JOIN vendors v ON i.vendor_id = v.id
            ORDER BY p.payment_date DESC NULLS LAST
            LIMIT 25;
        """
        return jsonify({"rows": run_query(sql)})
    except Exception as e:
        logging.exception("/cash-outflow error")
        return jsonify({"error": str(e)}), 500



@app.route("/ask", methods=["POST"])
def ask():
    if not use_llm:
        return jsonify({"error": "LLM not configured. Set GROQ_API_KEY."}), 400
    try:
        data = request.get_json(force=True)
        question = data.get("question", "").strip()
        if not question:
            return jsonify({"error": "No question provided."}), 400
        raw = sql_generator.invoke({"question": question})
        sql_text = extract_sql(raw)
        logging.info(f"🧠 Generated SQL:\n{sql_text}")
        if not is_safe_select(sql_text):
            return jsonify({"error": "Unsafe SQL generated.", "generated_sql": sql_text}), 400
        result = sql_runner_tool.invoke(sql_text)
        return jsonify({"question": question, "generated_sql": sql_text, "result": result})
    except Exception as e:
        logging.exception("/ask error")
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal(e):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
        
