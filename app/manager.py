import os
import redis
import boto3
import certifi
import uuid 
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from motor.motor_asyncio import AsyncIOMotorClient
from app.crud.neon import NeonCRUD
from app.crud.aiven import AivenCRUD
from app.crud.mongo import MongoCRUD
from app.crud.redis import RedisCRUD
from app.crud.dynamo import DynamoCRUD

class UniversalManager:
    def __init__(self):
        load_dotenv()
        
        required_vars = ["NEON_URL", "AIVEN_URL", "MONGO_URL", "REDIS_URL"]
        for var in required_vars:
            if not os.getenv(var):
                print(f"CRITICAL ERROR: {var} is missing from the .env file.")

        print("Initializing Distributed Database Manager...")
        
        self.neon_engine = create_engine(os.getenv("NEON_URL"), pool_pre_ping=True)
        self.NeonSession = sessionmaker(bind=self.neon_engine)

        self.aiven_engine = create_engine(os.getenv("AIVEN_URL"), pool_pre_ping=True)
        self.AivenSession = sessionmaker(bind=self.aiven_engine)

        self.mongo_client = AsyncIOMotorClient(os.getenv("MONGO_URL"), tlsCAFile=certifi.where())
        self.mongo_db = self.mongo_client.get_database("distributed_db")

        self.redis_client = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True, ssl_cert_reqs=None)

        self.dynamo_resource = boto3.resource(
            'dynamodb',
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        # Automatically check and create all missing tables in AWS
        self._ensure_dynamo_tables_exist()

        self.handlers = {
            "neon": NeonCRUD(self),
            "aiven": AivenCRUD(self),
            "mongo": MongoCRUD(self),
            "redis": RedisCRUD(self),
            "dynamo": DynamoCRUD(self)
        }

        self.route_map = {
            "users": "neon",      
            "inventory": "aiven",  
            "logs": "neon",       
            "orders": "aiven",     
            "sessions": "redis",
            "products": "mongo"
        }

    def _ensure_dynamo_tables_exist(self):
        try:
            # Get a list of tables that already exist in AWS
            existing_tables = [table.name for table in self.dynamo_resource.tables.all()]
            
            # List all the tables your project uses
            required_tables = ["users", "inventory", "logs", "orders", "sessions", "products"]
            
            for t_name in required_tables:
                if t_name not in existing_tables:
                    print(f"Creating '{t_name}' table in DynamoDB. Please wait...")
                    table = self.dynamo_resource.create_table(
                        TableName=t_name,
                        KeySchema=[
                            {'AttributeName': 'id', 'KeyType': 'HASH'} 
                        ],
                        AttributeDefinitions=[
                            {'AttributeName': 'id', 'AttributeType': 'S'} 
                        ],
                        BillingMode='PAY_PER_REQUEST'
                    )
                    
                    # Wait until AWS finishes creating this specific table before moving on
                    table.meta.client.get_waiter('table_exists').wait(TableName=t_name)
                #    print(f"Success! The '{t_name}' table is now active in DynamoDB.")
                
        except Exception as e:
            print(f" DynamoDB Table Setup Error: {e}")

    def _get_handler(self, table):
        db_type = self.route_map.get(table, "neon")
        return self.handlers[db_type], db_type

    async def create(self, table, data):
        handler, db_type = self._get_handler(table)
        
        if db_type == "mongo":
            res_id = await handler.create(table, data)
        elif db_type == "redis":
            new_id = str(uuid.uuid4())[:8]
            redis_key = f"{table}:{new_id}"
            handler.create(redis_key, data)
            res_id = new_id
        else:
            res_id = handler.create(table, data)
            
        return res_id, db_type

    async def read(self, table, item_id):
        handler, db_type = self._get_handler(table)
        search_id = int(item_id) if db_type in ["neon", "aiven"] else item_id
        
        if db_type == "mongo":
            return await handler.read(table, search_id)
        elif db_type == "redis":
            return handler.read(f"{table}:{search_id}")
        elif db_type == "dynamo":
            return handler.read(table, {"id": str(search_id)})
        return handler.read(table, search_id)

    async def update(self, table, item_id, data):
        handler, db_type = self._get_handler(table)
        update_id = int(item_id) if db_type in ["neon", "aiven"] else item_id
        
        if db_type == "mongo":
            return await handler.update(table, update_id, data)
        elif db_type == "redis":
            return handler.create(f"{table}:{item_id}", data) 
        elif db_type == "dynamo":
            return handler.update(table, {"id": str(item_id)}, data)
        return handler.update(table, update_id, data)

    async def delete(self, table, item_id):
        handler, db_type = self._get_handler(table)
        del_id = int(item_id) if db_type in ["neon", "aiven"] else item_id
        
        if db_type == "mongo":
            return await handler.delete(table, del_id)
        elif db_type == "redis":
            return handler.delete(f"{table}:{item_id}")
        elif db_type == "dynamo":
            return handler.delete(table, {"id": str(item_id)})
        return handler.delete(table, del_id)

    def get_neon_session(self): return self.NeonSession()
    def get_aiven_session(self): return self.AivenSession()
    def get_mongo_db(self): return self.mongo_db
    def get_redis_client(self): return self.redis_client
    def get_dynamo_resource(self): return self.dynamo_resource