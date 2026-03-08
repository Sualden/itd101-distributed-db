from enum import Enum
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy import text
import uvicorn
import uuid 

from app.manager import UniversalManager

# ==========================================
# API METADATA FOR DOCUMENTATION
# ==========================================
description = """
**ITD101 Distributed Database System API** 🚀

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
    {"name": "Danger Zone", "description": "Destructive commands to wipe databases."}
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

@app.get("/directory", tags=["System Info"], summary="Get Database Routing Map")
def get_database_directory():
    """Displays the active nodes and the routing map dictating which tables belong to which databases."""
    return {
        "message": "Global Distributed Database Routing Directory",
        "total_nodes_active": len(db_manager.handlers),
        "data_locations": db_manager.route_map
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
                res_id = await handler.create(table, data)
            elif target_val == "redis": 
                new_id = str(uuid.uuid4())[:8]
                handler.create(f"{table}:{new_id}", data)
                res_id = new_id
            elif target_val == "dynamo":
                # FIX: DynamoDB cannot auto-generate IDs. We must generate and inject one!
                new_id = str(uuid.uuid4())[:8]
                data["id"] = new_id
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

# ==========================================
# DANGER ZONE
# ==========================================
@app.delete("/wipe/{table}", tags=["Danger Zone"], summary="Wipe Table & Reset IDs")
async def wipe_all_data(table: str):
    """
    ⚠️ **DANGEROUS** ⚠️
    This action will securely connect to all 5 architectures (PostgreSQL, MySQL, MongoDB, Redis, DynamoDB), 
    delete EVERY record inside the requested table, and forcefully reset the SQL ID counters back to 1.
    """
    results = {}

    for db_name, session_maker in [("neon", db_manager.NeonSession), ("aiven", db_manager.AivenSession)]:
        session = session_maker()
        try:
            if db_name == "neon": session.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
            elif db_name == "aiven": session.execute(text(f"TRUNCATE TABLE {table}"))
            session.commit()
            results[db_name] = "Success: Table Cleared & IDs Reset to 1"
        except Exception as e:
            session.rollback()
            results[db_name] = f"Error: {str(e)}"
        finally:
            session.close()

    try:
        mongo_db = db_manager.get_mongo_db()
        del_result = await mongo_db[table].delete_many({})
        results["mongo"] = f"Success: Wiped {del_result.deleted_count} documents"
    except Exception as e: results["mongo"] = f"Error: {str(e)}"

    try:
        redis_client = db_manager.get_redis_client()
        keys = redis_client.keys(f"{table}:*")
        if keys: redis_client.delete(*keys); results["redis"] = f"Success: Wiped {len(keys)} keys"
        else: results["redis"] = "Success: No keys found"
    except Exception as e: results["redis"] = f"Error: {str(e)}"

    try:
        dynamo_table = db_manager.get_dynamo_resource().Table(table)
        response = dynamo_table.scan()
        items = response.get('Items', [])
        if items:
            with dynamo_table.batch_writer() as batch:
                for item in items: batch.delete_item(Key={'id': item['id']})
            results["dynamo"] = f"Success: Wiped {len(items)} items"
        else: results["dynamo"] = "Success: No items found"
    except Exception as e: results["dynamo"] = f"Error: {str(e)}"

    return {
        "message": f"🚨 GLOBAL WIPE & ID RESET EXECUTED FOR TABLE: {table.upper()} 🚨",
        "results": results
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=9090, reload=True)