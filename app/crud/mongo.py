import time
from bson.objectid import ObjectId

class MongoCRUD:
    def __init__(self, manager):
        self.manager = manager

    async def create(self, collection, document):
        db = self.manager.get_mongo_db()
        result = await db[collection].insert_one(document)
        return document.get("id", str(result.inserted_id))
    async def read(self, collection, doc_id):
        db = self.manager.get_mongo_db()
        try:
            try:
                search_val = int(doc_id)
            except ValueError:
                search_val = doc_id

            doc = await db[collection].find_one({"id": search_val})
            
            if not doc:
                doc = await db[collection].find_one({"_id": ObjectId(doc_id)})
                
            if doc: 
                doc['_id'] = str(doc['_id']) 
            return doc
        except Exception as e: 
            return None

    async def update(self, collection, doc_id, data):
        db = self.manager.get_mongo_db()
        try:
            try: search_val = int(doc_id)
            except ValueError: search_val = doc_id
            result = await db[collection].update_one({"id": search_val}, {"$set": data})
            
            if result.matched_count == 0:
                result = await db[collection].update_one({"_id": ObjectId(doc_id)}, {"$set": data})
                
            return {"modified_count": result.modified_count}
        except: 
            return {"modified_count": 0}

    async def delete(self, collection, doc_id):
        db = self.manager.get_mongo_db()
        try:
            try: search_val = int(doc_id)
            except ValueError: search_val = doc_id
            result = await db[collection].delete_one({"id": search_val})
            
            if result.deleted_count == 0:
                result = await db[collection].delete_one({"_id": ObjectId(doc_id)})
                
            return {"deleted_count": result.deleted_count}
        except: 
            return {"deleted_count": 0}
        
    async def read_all(self, collection):
        db = self.manager.get_mongo_db()
        try:
            cursor = db[collection].find({})
            docs = await cursor.to_list(length=1000)
            for doc in docs:
                doc['_id'] = str(doc['_id'])
            return docs
        except Exception as e: 
            return []   