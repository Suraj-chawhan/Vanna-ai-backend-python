import re
import json
import os
import pandas as pd
from flask import Flask, request, jsonify, send_from_directory
from langchain_community.utilities import SQLDatabase
from langchain_groq import ChatGroq
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool

app = Flask(__name__)

# -------------------------
# Database + LLM Config
# -------------------------
DB_URL = os.getenv("DB_URL")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

db = SQLDatabase.from_uri(DB_URL)
llm = ChatGroq(model="openai/gpt-oss-20b", groq_api_key=GROQ_API_KEY, temperature=0.1)
generate_query = create_sql_query_chain(llm, db)
run_query = QuerySQLDataBaseTool(db=db)


def clean_sql_output(raw_text: str) -> str:
    """Extract clean SQL text from LLM output"""
    match = re.search(r"```sql(.*?)```", raw_text, re.DOTALL)
    if match:
        sql = match.group(1)
    else:
        sql = raw_text
    sql = re.sub(r'(?i)question:.*?\n', '', sql)
    sql = re.sub(r'(?i)sqlquery:', '', sql)
    return sql.strip()

# -------------------------
# API ROUTES
# -------------------------

@app.route("/ask", methods=["POST"])
def ask_question():
    data = request.get_json()
    question = data.get("question")

    if not question:
        return jsonify({"error": "No question provided"}), 400

    raw_sql = generate_query.invoke({"question": question})
    sql_query = clean_sql_output(raw_sql)

    try:
        result = run_query.invoke(sql_query)
        return jsonify({
            "question": question,
            "generated_sql": sql_query,
            "result": result
        })
    except Exception as e:
        return jsonify({
            "question": question,
            "generated_sql": sql_query,
            "error": str(e)
        }), 500


@app.route("/stats")
def stats():
    """Return aggregated vendor stats for chart display"""
    try:
        sql = """
        SELECT v.vendor_name, SUM(i.invoice_total) AS total
        FROM invoices i
        JOIN vendors v ON i.vendor_id = v.id
        GROUP BY v.vendor_name
        ORDER BY total DESC
        LIMIT 10;
        """
        df = pd.read_sql_query(sql, db._engine)
        return jsonify({
            "labels": df["vendor_name"].tolist(),
            "values": df["total"].tolist()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -------------------------
# Serve HTML (root folder)
# -------------------------
@app.route("/")
def serve_root():
    return send_from_directory(".", "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
    
