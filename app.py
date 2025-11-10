#!/usr/bin/env python3
import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, send_from_directory

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL must be set (postgres://...)")

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

@app.route("/")
def index():
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

@app.route("/stats")
def stats():
    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT COUNT(*) AS total_invoices,
                   COALESCE(SUM(invoice_total), 0) AS total_revenue,
                   COALESCE(AVG(invoice_total), 0) AS avg_invoice
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
        return jsonify({"monthly": run_select(sql)})
    except Exception as e:
        logging.exception("/invoice-trends error")
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
            ORDER BY p.payment_date DESC
            LIMIT 25;
        """
        rows = run_select(sql)
        return jsonify({"rows": rows})
    except Exception as e:
        logging.exception("/cash-outflow error")
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
        
