ITD101 Distributed Database System 🚀
Student: Sualden S. Sala

Project: ITD101 Distributed Systems

Architecture: Client-Server

📖 Project Overview
This project is a centralized API gateway built with FastAPI that manages data across five completely different database systems. Instead of interacting with each database separately, this system uses a UniversalManager to handle connections, route data automatically, and broadcast CRUD (Create, Read, Update, Delete) operations globally.

🏗️ Supported Databases
The system connects to the following 5 nodes simultaneously:

Neon (PostgreSQL): Relational database

Aiven (MySQL): Relational database

MongoDB: Document-based NoSQL database

Redis: In-memory Key-Value store

AWS DynamoDB: Wide-column NoSQL database

⚙️ Core Mechanics
The API uses a unified JSON payload (UniversalPayload) containing a table name and data. Operations are controlled via a dropdown target parameter in the Swagger UI:

target="global": Broadcasts the operation to all 5 databases at once. For inserts, it generates a single ID from the primary SQL database and injects it into the NoSQL databases to maintain synchronized IDs.

target="routed" (Default): The UniversalManager checks its internal route_map and automatically sends the data to the correct database based on the table name.

Current Route Map: users ➔ Neon, inventory ➔ Aiven, logs ➔ Neon, orders ➔ Aiven, sessions ➔ Redis, products ➔ MongoDB.

target="[db_name]": Ignores the route map and forces the operation on a specific node (e.g., target="mongo").

📡 API Endpoints
GET / - System Health Check & Student Info.

GET /directory - Displays the active nodes and the route_map.

POST /create - Inserts new data.

GET /{table}/{id} - Retrieves data by ID.

PUT /{table}/{id} - Updates existing data by ID.

DELETE /{table}/{id} - Deletes a specific record by ID.

DELETE /wipe/{table} - ⚠️ DANGER ZONE: Connects to all 5 databases, wipes all data in the specified table, and uses TRUNCATE to reset SQL auto-increment counters back to 1.

📝 Example Data Payloads
When using the POST /create or PUT /{table}/{id} endpoints in the Swagger UI, you must provide a JSON payload. The system will automatically handle the ID generation for you.

Example 1: Creating a User (SQL Data) If target="routed", this automatically goes to Neon.

JSON
{
  "table": "users",
  "data": {
    "username": "admin_user",
    "email": "admin@example.com"
  }
}
Example 2: Creating a Product (NoSQL Data) If target="routed", this automatically goes to MongoDB.

JSON
{
  "table": "products",
  "data": {
    "product_name": "Mechanical Keyboard",
    "price": 120.50,
    "stock": 45,
    "category": "electronics"
  }
}
🛠️ Setup Instructions
1. Create your .env file:
The UniversalManager requires the following credentials in the root folder. Never commit this file to GitHub!

Code snippet
NEON_URL="postgresql://..."
AIVEN_URL="mysql+pymysql://..."
MONGO_URL="mongodb+srv://..."
REDIS_URL="rediss://..."

AWS_REGION="your-region"
AWS_ACCESS_KEY_ID="your-access-key"
AWS_SECRET_ACCESS_KEY="your-secret-key"
2. Run the Server:
Start the FastAPI application using Uvicorn:

Bash
python main.py
The server will run on http://0.0.0.0:9090.

3. Access the Interactive UI:
Go to http://localhost:9090/docs in your browser to test the endpoints and use the target dropdowns.
