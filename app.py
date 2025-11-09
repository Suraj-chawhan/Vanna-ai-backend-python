import os
import re
import json
import pandas as pd
from flask import Flask, request, jsonify, send_from_directory
from langchain_community.utilities import SQLDatabase
from langchain_groq import ChatGroq
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool

# -------------------------
# Flask App
# -------------------------
app = Flask(__name__)

# -------------------------
# Config
# -------------------------
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://hunny:QTSFnZ96Zd7VdBTU6wqleJ8pXfPG97Ga@dpg-d47dbpili9vc738l3n00-a.singapore-postgres.render.com/flowbit"
)
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "gsk_YOUR_GROQ_KEY_HERE")

# -------------------------
# Initialize LangChain + DB
# -------------------------
db = SQLDatabase.from_uri(DB_URL)

# ✅ use Groq with openai/gpt-oss-20b
llm = ChatGroq(
    model="openai/gpt-oss-20b",
    groq_api_key=GROQ_API_KEY,
    temperature=0.1
)

generate_query = create_sql_query_chain(llm, db)
run_query = QuerySQLDataBaseTool(db=db)

# -------------------------
# Helper: clean SQL text
# -------------------------
def clean_sql_output(raw_text: str) -> str:
    """Extract clean SQL from the model output."""
    match = re.search(r"```sql(.*?)```", raw_text, re.DOTALL)
    sql = match.group(1) if match else raw_text
    sql = re.sub(r'(?i)question:.*?\n', '', sql)
    sql = re.sub(r'(?i)sqlquery:', '', sql)
    return sql.strip()

# -------------------------
# /ask → Generate SQL & Query
# -------------------------
@app.route("/ask", methods=["POST"])
def ask_question():
    data = request.get_json()
    question = data.get("question")
    if not question:
        return jsonify({"error": "No question provided"}), 400

    try:
        # 1. Generate SQL
        raw_sql = generate_query.invoke({"question": question})
        sql_query = clean_sql_output(raw_sql)

        # 2. Execute SQL
        result = run_query.invoke(sql_query)

        return jsonify({
            "question": question,
            "generated_sql": sql_query,
            "result": result
        })
    except Exception as e:
        return jsonify({
            "question": question,
            "error": str(e)
        }), 500


# -------------------------
# /stats → Dashboard Data
# -------------------------
@app.route("/stats", methods=["GET"])
def stats():
    """
    Dashboard metrics from your invoices schema.
    Returns:
      - top vendors
      - total revenue
      - total invoices
      - avg invoice value
      - total customers
    """
    try:
        queries = {
            "top_vendors": """
                SELECT v.vendor_name, COALESCE(SUM(i.invoice_total),0) AS total
                FROM invoices i
                JOIN vendors v ON i.vendor_id = v.id
                GROUP BY v.vendor_name
                ORDER BY total DESC
                LIMIT 10;
            """,
            "totals": """
                SELECT
                    COUNT(*) AS total_invoices,
                    COALESCE(SUM(invoice_total),0) AS total_revenue,
                    AVG(invoice_total) AS avg_invoice
                FROM invoices;
            """,
            "customers": "SELECT COUNT(*) AS total_customers FROM customers;"
        }

        results = {}
        for key, sql in queries.items():
            df = pd.read_sql_query(sql, db._engine)
            results[key] = df.to_dict(orient="records")

        # Flatten the numeric stats for easy use in frontend
        totals = results["totals"][0] if results["totals"] else {}
        customer_count = results["customers"][0].get("total_customers", 0)

        return jsonify({
            "top_vendors": results["top_vendors"],
            "stats_summary": {
                "total_invoices": totals.get("total_invoices", 0),
                "total_revenue": float(totals.get("total_revenue", 0)),
                "avg_invoice": float(totals.get("avg_invoice", 0)),
                "total_customers": customer_count
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -------------------------
# Serve HTML (root)
# -------------------------
@app.route("/", methods=["GET"])
def serve_root():
    return send_from_directory(".", "index.html")


# -------------------------
# Entry
# -------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
    
