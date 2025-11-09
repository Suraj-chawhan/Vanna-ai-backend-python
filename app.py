# app.py
import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from vanna.integrations.google import GeminiLlmService
from vanna.integrations.postgres import PostgresRunner
from vanna.tools import RunSqlTool

# Load environment variables
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL")

if not GEMINI_API_KEY or not POSTGRES_URL:
    raise ValueError("Missing GEMINI_API_KEY or POSTGRES_URL!")

app = Flask(__name__)

# Initialize Gemini
llm = GeminiLlmService(model="gemini-1.5-flash", api_key=GEMINI_API_KEY)

# PostgreSQL tool
db_tool = RunSqlTool(sql_runner=PostgresRunner(connection_string=POSTGRES_URL))

@app.route("/ask", methods=["POST"])
def ask_question():
    data = request.json
    question = data.get("question")
    if not question:
        return jsonify({"error": "No question provided"}), 400

    # Generate SQL from Gemini
    result = llm.generate_sql(question=question)
    sql_query = result["sql"]

    # Run SQL
    try:
        df = db_tool.run(sql_query)
        return jsonify({
            "question": question,
            "sql": sql_query,
            "result": df.to_dict(orient="records")
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
    
