### **ITD101 Distributed Database System**

**Author:** Sualden S. Sala  
**Course:** ITD101

---

## **📌 Project Overview**
This project is a **Universal Database Manager** built with FastAPI that routes data across five distinct database architectures. It allows for seamless data management by treating multiple specialized databases as a single unified system.



### **The 5-Node Architecture**
1.  **PostgreSQL (Neon):** Relational SQL used for core identity and event tracking like `users` and `logs`.
2.  **MySQL (Aiven):** Relational SQL used for commerce-related data like `inventory` and `orders`.
3.  **MongoDB:** Document-based NoSQL for flexible data structures like `products`.
4.  **Redis:** In-memory Key-Value store for `sessions`. It also acts as the **Global Sequence Generator**, providing clean, counting IDs (1, 2, 3...) for all NoSQL nodes.
5.  **AWS DynamoDB:** Wide-column NoSQL for high-scale cloud storage.

---

## **✨ Key Features**
* **Intelligent Routing (`routed` mode):** Automatically directs data to the correct database based on the table name defined in the `route_map`.
* **Global Operations (`global` mode):** Allows you to broadcast a single "Create" request to all five databases at once or perform a "Global Search" that checks every node for a specific ID.
* **Sequential ID Logic:** Uses Redis to ensure that even NoSQL databases like Mongo and DynamoDB use incrementing integer IDs instead of random strings.
* **Dynamic Security Whitelisting:** Each database handler automatically identifies which tables it is allowed to manage by looking at the central routing configuration.
* **Master System Dump:** A diagnostic tool (`/directory/all`) that loops through all five databases and returns every single record in one combined JSON view.
* **Auto-Schema Initialization:** On startup, the system automatically verifies and creates the necessary tables in Neon, Aiven, and AWS DynamoDB if they do not already exist.

---

## **🚀 Getting Started**

### **1. Setup Environment**
Create a `.env` file in the root directory with your credentials:
```env
NEON_URL="postgresql://user:pass@host/dbname"
AIVEN_URL="mysql+pymysql://user:pass@host:port/dbname"
MONGO_URL="mongodb+srv://..."
REDIS_URL="redis://..."
AWS_REGION="ap-southeast-1"
AWS_ACCESS_KEY_ID="your_key"
AWS_SECRET_ACCESS_KEY="your_secret"
```

### **2. Installation**
```bash
pip install fastapi uvicorn sqlalchemy pymysql psycopg2-binary motor redis boto3 python-dotenv certifi
```

### **3. Running the Server**
```bash
python main.py
```
The server will start on `http://localhost:9090`.

---

## **📡 API Usage**

### **Universal Create (`POST /create`)**
By default, the system uses **Routed Mode**. You only need to provide the table name and the data:
* **Table:** `users` ➔ Saved to **Neon**.
* **Table:** `orders` ➔ Saved to **Aiven**.
* **Table:** `products` ➔ Saved to **MongoDB**.

### **System Diagnostics**
* `GET /directory`: View the active routing map.
* `GET /directory/{db_name}`: Dump data from a specific database (e.g., `/directory/dynamo`).
* `GET /directory/all`: **The Master Dump** – View all data in the entire distributed system.

---

**Interactive API Documentation:** Available at `http://localhost:9090/docs`.
