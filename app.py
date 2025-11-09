import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from vanna import Agent
from vanna.core.registry import ToolRegistry
from vanna.core.user import UserResolver, User, RequestContext
from vanna.integrations.google import GeminiLlmService
from vanna.integrations.postgres import PostgresRunner
from vanna.tools import RunSqlTool
from vanna.tools.agent_memory import DemoAgentMemory

# -------------------------------
# 1️⃣ Load environment variables
# -------------------------------
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL")
PORT = int(os.getenv("PORT", 5000))

if not GEMINI_API_KEY or not POSTGRES_URL:
    raise ValueError("Missing GEMINI_API_KEY or POSTGRES_URL in .env file!")

# -------------------------------
# 2️⃣ Initialize Flask
# -------------------------------
app = Flask(__name__)

# -------------------------------
# 3️⃣ Initialize Gemini LLM
# -------------------------------
llm = GeminiLlmService(
    model="gemini-1.5-flash",
    api_key=GEMINI_API_KEY
)
print("✅ Gemini LLM initialized")

# -------------------------------
# 4️⃣ PostgreSQL Tool
# -------------------------------
db_tool = RunSqlTool(
    sql_runner=PostgresRunner(connection_string=POSTGRES_URL)
)
print("✅ PostgreSQL tool ready")

# -------------------------------
# 5️⃣ Agent Memory
# -------------------------------
agent_memory = DemoAgentMemory(max_items=1000)

# -------------------------------
# 6️⃣ User Resolver
# -------------------------------
class SimpleUserResolver(UserResolver):
    async def resolve_user(self, request_context: RequestContext) -> User:
        # For demo: assign all users to 'admin' group
        user_email = request_context.get_cookie("vanna_email") or "guest@example.com"
        return User(id=user_email, email=user_email, group_memberships=["admin"])

user_resolver = SimpleUserResolver()

# -------------------------------
# 7️⃣ Register tools
# -------------------------------
tools = ToolRegistry()
tools.register_local_tool(db_tool, access_groups=["admin", "user"])

# -------------------------------
# 8️⃣ Create Agent
# -------------------------------
agent = Agent(
    llm_service=llm,
    tool_registry=tools,
    user_resolver=user_resolver,
    agent_memory=agent_memory
)

# -------------------------------
# 9️⃣ Flask route to ask question
# -------------------------------
@app.route("/ask", methods=["POST"])
def ask_question():
    data = request.json
    question = data.get("question")
    if not question:
        return jsonify({"error": "No question provided"}), 400

    try:
        # Use agent.chat() instead of deprecated query() or generate_sql()
        response = agent.chat(question)
        return jsonify({
            "question": question,
            "answer": response.output_text,
            "data": getattr(response, "output_data", None)  # Optional DB results
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -------------------------------
# 10️⃣ Run Flask server
# -------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=True)
