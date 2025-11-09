import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from vanna import Agent
from vanna.core.registry import ToolRegistry
from vanna.core.user import UserResolver, User, RequestContext
from vanna.tools import RunSqlTool
from vanna.integrations.google import GeminiLlmService
from vanna.integrations.postgres import PostgresRunner
from vanna.integrations.local.agent_memory import DemoAgentMemory

# Load env
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

# Agent memory
agent_memory = DemoAgentMemory(max_items=1000)

# Simple user resolver
class SimpleUserResolver(UserResolver):
    async def resolve_user(self, request_context: RequestContext) -> User:
        user_email = request_context.get_cookie('vanna_email') or 'guest@example.com'
        group = 'admin' if user_email == 'admin@example.com' else 'user'
        return User(id=user_email, email=user_email, group_memberships=[group])

user_resolver = SimpleUserResolver()

# Register tools
tools = ToolRegistry()
tools.register_local_tool(db_tool, access_groups=['admin','user'])

# Create Agent
agent = Agent(
    llm_service=llm,
    tool_registry=tools,
    user_resolver=user_resolver,
    agent_memory=agent_memory
)

# Flask route
@app.route("/ask", methods=["POST"])
def ask_question():
    data = request.json
    question = data.get("question")
    if not question:
        return jsonify({"error": "No question provided"}), 400
    try:
        # Ask Agent
        result = agent.query(question)
        return jsonify({
            "question": question,
            "sql": result.sql,
            "result": result.data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
    
