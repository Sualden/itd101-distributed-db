from enum import Enum
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy import text
import uvicorn
import time


from app.manager import UniversalManager

# ==========================================
# API METADATA FOR DOCUMENTATION
# ==========================================
description = """
**ITD101 Distributed Database System API** 

This API acts as a universal manager routing data across 5 distinct database architectures:
* **Relational (SQL):** PostgreSQL (Neon) & MySQL (Aiven)
* **Document (NoSQL):** MongoDB
* **Key-Value (In-Memory):** Redis
* **Wide-Column (NoSQL):** DynamoDB (AWS)

### Features:
* **Global Mode:** Broadcast or query data across all 5 databases simultaneously.
* **Routed Mode:** Automatically direct data to the optimal database based on table configuration.
* **Direct Mode:** Force queries to execute on a specific, targeted database node.
"""

tags_metadata = [
    {"name": "System Info", "description": "Check system health and database routing rules."},
    {"name": "CRUD Operations", "description": "Create, Read, Update, and Delete data globally or specifically."},
 #     {"name": "Danger Zone", "description": "Destructive commands to wipe databases."}
]

app = FastAPI(
    title="ITD101 Distributed Database System",
    description=description,
    version="1.0.0",
    contact={
        "name": "Sualden S. Sala",
        "email": "sualden@example.com",
    },
    openapi_tags=tags_metadata
)

db_manager = UniversalManager()

# ==========================================
# MODELS & ENUMS
# ==========================================
class UniversalPayload(BaseModel):
    table: str = Field(..., description="The name of the database table or collection (e.g., 'users', 'products')")
    data: Dict[str, Any] = Field(..., description="A JSON dictionary containing the data fields to insert/update")

class TargetNode(str, Enum):
    GLOBAL = "global"
    ROUTED = "routed"
    NEON = "neon"
    AIVEN = "aiven"
    MONGO = "mongo"
    REDIS = "redis"
    DYNAMO = "dynamo"

# ==========================================
# SYSTEM INFO ENDPOINTS
# ==========================================
@app.get("/", tags=["System Info"], summary="System Health Check")
def root():
    """Returns basic information about the project and its operational status."""
    return {
        "Project": "ITD101 Distributed Systems",
        "Student": "Sualden S. Sala",
        "Status": "System Operational",
        "Architecture": "Client-Server"
    }

@app.get("/directory/{db_name}", tags=["System Info"], summary="Dump All Data from a Database")
async def get_all_db_data(db_name: str):
    """
    **Diagnostic Tool:** Fetches every single record from every single table.
    Use /directory/all to dump the ENTIRE distributed system at once!
    """
    all_tables = ["users", "inventory", "logs", "orders", "sessions", "products"]
    
    # 1. THE NEW "MASTER DUMP" FEATURE
    if db_name == "all":
        master_dump = {}
        for name, handler in db_manager.handlers.items():
            db_data = {}
            
            for table in all_tables:
                try:
                    if name == "mongo":
                        db_data[table] = await handler.read_all(table)
                    else:
                        db_data[table] = handler.read_all(table)
                except Exception as e:
                    db_data[table] = f"Error: {str(e)}"
            
            master_dump[name.upper()] = db_data
            
        return {
            "status": "Success",
            "node": "GLOBAL_SYSTEM_DUMP",
            "total_databases_scanned": len(db_manager.handlers),
            "data": master_dump
        }
    if db_name not in db_manager.handlers:
        raise HTTPException(
            status_code=404, 
            detail=f"Database '{db_name}' not found. Available nodes: {list(db_manager.handlers.keys())} or use 'all'"
        )
    
    handler = db_manager.handlers[db_name]
    database_dump = {}
    
    for table in all_tables:
        try:
            if db_name == "mongo":
                database_dump[table] = await handler.read_all(table)
            else:
                database_dump[table] = handler.read_all(table)
        except Exception as e:
            database_dump[table] = f"Could not fetch: {str(e)}"
            
    return {
        "status": "Success",
        "node": db_name.upper(),
        "tables_scanned": len(all_tables),
        "data": database_dump
    }

# ==========================================
# CRUD OPERATIONS
# ==========================================
@app.post("/create", tags=["CRUD Operations"], summary="Create Data")
async def create_data(
    payload: UniversalPayload, 
    target: TargetNode = Query(TargetNode.ROUTED, description="Select how/where to insert the data")
):
    """
    **Insert new data into the system.**
    - If `global`: Inserts into the primary SQL DB to generate a counting ID, then copies it to all others.
    - If `routed`: Lets the Universal Manager auto-assign the DB based on the table name.
    - If `specific` (e.g., 'mongo'): Forces insertion into that specific database.
    """
    table = payload.table
    data = payload.data.copy()
    target_val = target.value

    if target_val == "global":
        results = {}
        primary_handler, primary_db_name = db_manager._get_handler(table)
        try:
            global_id, _ = await db_manager.create(table, data)
            results[primary_db_name] = {"status": "Success", "inserted_id": global_id}
        except Exception as e:
            return {"message": f"Broadcast failed at primary DB ({primary_db_name})", "error": str(e)}

        for db_name, handler in db_manager.handlers.items():
            if db_name == primary_db_name: continue
            db_data = data.copy()
            try:
                if db_name == "mongo": db_data["id"] = global_id; await handler.create(table, db_data); res = global_id
                elif db_name == "redis": handler.create(f"{table}:{global_id}", db_data); res = global_id
                elif db_name == "dynamo": db_data["id"] = str(global_id); handler.create(table, db_data); res = global_id
                else: db_data["id"] = global_id; handler.create(table, db_data); res = global_id
                results[db_name] = {"status": "Success", "inserted_id": res}
            except Exception as e:
                results[db_name] = {"status": "Failed", "error": str(e)}
        return {"message": "Global Data Broadcast Complete!", "table": table, "global_id": global_id, "results": results}
    
    elif target_val == "routed":
        try:
            res_id, db_name = await db_manager.create(table, data)
            return {"status": "Success", "message": f"Data routed to {db_name.upper()} database", "table": table, "id": res_id, "node": db_name}
        except IntegrityError: raise HTTPException(409, detail="Data conflict: A record with this unique data already exists.")
        except ProgrammingError: raise HTTPException(400, detail="Invalid data schema: Check your column names.")
        except Exception as e: raise HTTPException(500, detail=str(e))
            
  
    elif target_val in db_manager.handlers:
        handler = db_manager.handlers[target_val]
        try:
            if target_val == "mongo": 
                if "id" not in data:
                    data["id"] = db_manager.redis_client.incr(f"{table}_sequence")
                res_id = await handler.create(table, data)
            elif target_val == "redis": 
                new_id = db_manager.redis_client.incr(f"{table}_sequence")
                handler.create(f"{table}:{new_id}", data)
                res_id = new_id
            elif target_val == "dynamo":
                new_id = db_manager.redis_client.incr(f"{table}_sequence")
                data["id"] = str(new_id)  # <--- FORCE IT TO BE A STRING HERE
                handler.create(table, data)
                res_id = new_id
            else: 
                res_id = handler.create(table, data)
                
            return {"status": "Success", "message": f"Data explicitly saved to {target_val.upper()}", "node": target_val, "id": res_id}
        except IntegrityError: raise HTTPException(409, detail="Data conflict: A record with this unique data already exists.")
        except ProgrammingError: raise HTTPException(400, detail="Invalid data schema: Check your column names.")
        except Exception as e: raise HTTPException(500, detail=str(e))
    else:
        raise HTTPException(400, detail="Invalid target parameter.")
    
@app.get("/{table}/{id}", tags=["CRUD Operations"], summary="Read Data")
async def read_data(
    table: str, 
    id: str, 
    target: TargetNode = Query(TargetNode.GLOBAL, description="Select where to pull the data from")
):
    """**Retrieve data by its ID.** Use `global` to search across all databases at once."""
    target_val = target.value

    if target_val == "global":
        results = {}
        for db_name, handler in db_manager.handlers.items():
            try: target_id = int(id) if db_name in ["neon", "aiven"] else id
            except ValueError: results[db_name] = {"status": "Skipped", "error": "ID must be a number for SQL"}; continue 
            
            try:
                if db_name == "mongo": res = await handler.read(table, target_id)
                elif db_name == "redis": res = handler.read(f"{table}:{target_id}")
                elif db_name == "dynamo": res = handler.read(table, {"id": str(target_id)})
                else: res = handler.read(table, target_id)
                results[db_name] = {"status": "Found", "data": res} if res else {"status": "Not Found"}
            except Exception as e:
                results[db_name] = {"status": "Failed", "error": str(e)}
        return {"message": "Global Read completed", "table": table, "search_id": id, "results": results}
    
    elif target_val == "routed":
        res = await db_manager.read(table, id)
        _, db_type = db_manager._get_handler(table)
        return {"status": "Found" if res else "Not Found", "node": db_type, "data": res}
        
    elif target_val in db_manager.handlers:
        handler = db_manager.handlers[target_val]
        try:
            target_id = int(id) if target_val in ["neon", "aiven"] else id
            if target_val == "mongo": res = await handler.read(table, target_id)
            elif target_val == "redis": res = handler.read(f"{table}:{target_id}")
            elif target_val == "dynamo": res = handler.read(table, {"id": str(target_id)})
            else: res = handler.read(table, target_id)
            return {"status": "Found" if res else "Not Found", "node": target_val, "data": res}
        except ValueError: raise HTTPException(400, detail="ID must be a number for SQL databases.")
        except Exception as e: raise HTTPException(500, detail=str(e))
    else:
        raise HTTPException(400, detail="Invalid target parameter.")

@app.put("/{table}/{id}", tags=["CRUD Operations"], summary="Update Data")
async def update_data(
    table: str, 
    id: str, 
    payload: UniversalPayload, 
    target: TargetNode = Query(TargetNode.ROUTED, description="Select where to update the data")
):
    """**Update existing data by ID.** Provide the new JSON payload to overwrite existing fields."""
    data = payload.data.copy()
    target_val = target.value

    if target_val == "global":
        results = {}
        for db_name, handler in db_manager.handlers.items():
            try: target_id = int(id) if db_name in ["neon", "aiven"] else id
            except ValueError: results[db_name] = {"status": "Skipped", "error": "ID must be a number for SQL"}; continue
            
            try:
                db_data = data.copy()
                if db_name == "mongo": res = await handler.update(table, target_id, db_data)
                elif db_name == "redis": res = handler.create(f"{table}:{target_id}", db_data)
                elif db_name == "dynamo": res = handler.update(table, {"id": str(target_id)}, db_data)
                else: res = handler.update(table, target_id, db_data)
                results[db_name] = {"status": "Success", "result": res}
            except Exception as e:
                results[db_name] = {"status": "Failed", "error": str(e)}
        return {"message": "Global Update completed", "table": table, "id": id, "results": results}
        
    elif target_val == "routed":
        try:
            res = await db_manager.update(table, id, data)
            _, db_type = db_manager._get_handler(table)
            return {"message": f"Data successfully updated in the {db_type.upper()} database.", "node": db_type, "status": res}
        except Exception as e: raise HTTPException(500, detail=str(e))
            
    elif target_val in db_manager.handlers:
        handler = db_manager.handlers[target_val]
        try:
            target_id = int(id) if target_val in ["neon", "aiven"] else id
            if target_val == "mongo": res = await handler.update(table, target_id, data)
            elif target_val == "redis": res = handler.create(f"{table}:{target_id}", data)
            elif target_val == "dynamo": res = handler.update(table, {"id": str(target_id)}, data)
            else: res = handler.update(table, target_id, data)
            return {"message": f"Data explicitly updated in {target_val.upper()}", "node": target_val, "status": res}
        except ValueError: raise HTTPException(400, detail="ID must be a number for SQL databases.")
        except Exception as e: raise HTTPException(500, detail=str(e))
    else:
        raise HTTPException(400, detail="Invalid target parameter.")

@app.delete("/{table}/{id}", tags=["CRUD Operations"], summary="Delete Data")
async def delete_data(
    table: str, 
    id: str, 
    target: TargetNode = Query(TargetNode.ROUTED, description="Select where to delete the data from")
):
    """**Delete a single record by its ID.** Use `global` to permanently delete it from all 5 databases."""
    target_val = target.value

    if target_val == "global":
        results = {}
        for db_name, handler in db_manager.handlers.items():
            try: target_id = int(id) if db_name in ["neon", "aiven"] else id
            except ValueError: results[db_name] = {"status": "Skipped", "error": "ID must be a number for SQL"}; continue
            
            try:
                if db_name == "mongo": res = await handler.delete(table, target_id)
                elif db_name == "redis": res = handler.delete(f"{table}:{target_id}")
                elif db_name == "dynamo": res = handler.delete(table, {"id": str(target_id)})
                else: res = handler.delete(table, target_id)
                results[db_name] = {"status": "Success", "result": res}
            except Exception as e:
                results[db_name] = {"status": "Failed", "error": str(e)}
        return {"message": "Global Delete completed", "table": table, "id": id, "results": results}

    elif target_val == "routed":
        try:
            res = await db_manager.delete(table, id)
            _, db_type = db_manager._get_handler(table)
            return {"message": f"Data permanently deleted from the {db_type.upper()} database.", "node": db_type, "status": res}
        except Exception as e: raise HTTPException(500, detail=str(e))
        
    elif target_val in db_manager.handlers:
        handler = db_manager.handlers[target_val]
        try:
            target_id = int(id) if target_val in ["neon", "aiven"] else id
            if target_val == "mongo": res = await handler.delete(table, target_id)
            elif target_val == "redis": res = handler.delete(f"{table}:{target_id}")
            elif target_val == "dynamo": res = handler.delete(table, {"id": str(target_id)})
            else: res = handler.delete(table, target_id)
            return {"message": f"Data explicitly deleted from {target_val.upper()}", "node": target_val, "status": res}
        except ValueError: raise HTTPException(400, detail="ID must be a number for SQL databases.")
        except Exception as e: raise HTTPException(500, detail=str(e))
    else:
        raise HTTPException(400, detail="Invalid target parameter.")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=9090, reload=True)