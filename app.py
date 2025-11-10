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
GROQ_API_KEY = os.getenv("GROQ_API_KEY", None)
LLM_MODEL = os.getenv("LLM_MODEL", "openai/gpt-oss-20b")

# -------------------------
# APP + DB helpers
# -------------------------
app = Flask(__name__, static_folder=".", template_folder=".")

def get_conn():
    return psycopg2.connect(DB_URL, sslmode="require")

# -------------------------
# LLM Setup
# -------------------------
use_llm = GROQ_API_KEY is not None
if use_llm:
    sql_db = SQLDatabase.from_uri(DB_URL)
    llm = ChatGroq(model=LLM_MODEL, groq_api_key=GROQ_API_KEY, temperature=0.05)
    sql_generator = create_sql_query_chain(llm, sql_db)
    sql_runner_tool = QuerySQLDataBaseTool(db=sql_db)
else:
    sql_generator = None
    sql_runner_tool = None

# -------------------------
# SQL utilities
# -------------------------
SQL_FENCE_RE = re.compile(r"```sql(.*?)```", re.DOTALL | re.IGNORECASE)

def extract_sql(text: str) -> str:
    if not text:
        return ""
    m = SQL_FENCE_RE.search(text)
    sql = m.group(1) if m else text
    sql = re.sub(r'(?i)^question:.*$', '', sql, flags=re.MULTILINE).strip()
    sql = re.sub(r'(?i)^sqlquery:.*$', '', sql, flags=re.MULTILINE).strip()
    sql = sql.strip("` \n\r\t")
    return sql.strip()

def is_select_only(sql: str) -> bool:
    if not sql:
        return False
    s = sql.strip().lower()
    forbidden = ["insert ", "update ", "delete ", "drop ", "truncate ", "alter ", "create ", "grant ", "revoke ", "replace "]
    if any(k in s for k in forbidden):
        return False
    if ";" in s and s.count(";") > 1:
        return False
    return s.startswith("select") or s.startswith("with")

def run_select(sql: str):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        def fix(v):
            if isinstance(v, (int, float, str, bool)) or v is None:
                return v
            try:
                return float(v)
            except Exception:
                return v
        return [{k: fix(v) for k, v in row.items()} for row in rows]
    except Exception:
        cur.close()
        conn.close()
        raise

# -------------------------
# ROUTES
# -------------------------

@app.route("/stats")
def route_stats():
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
        top = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) AS total_invoices, COALESCE(SUM(invoice_total),0) AS total_revenue,
                   COALESCE(AVG(invoice_total),0) AS avg_invoice
            FROM invoices;
        """)
        totals = cur.fetchone()

        cur.execute("SELECT COUNT(*) AS total_customers FROM customers;")
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


@app.route("/invoice-trends")
def invoice_trends():
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
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"monthly": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/vendors/top10")
def vendors_top10():
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
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"vendors": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/category-spend")
def category_spend():
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
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"categories": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/cash-outflow")
def cash_outflow():
    """
    Returns forecast of upcoming cash outflows based on due dates.
    Buckets invoices into time ranges.
    """
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
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"buckets": rows})

    except Exception as e:
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
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({"invoices": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        if not is_select_only(sql_text):
            return jsonify({"error": "Unsafe SQL.", "generated_sql": sql_text}), 400
        result = sql_runner_tool.invoke(sql_text)
        return jsonify({"question": question, "generated_sql": sql_text, "result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
                          
