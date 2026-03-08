from sqlalchemy import text
import logging

class NeonCRUD:
    def __init__(self, manager):
        self.manager = manager
        self.allowed_tables = ["users", "inventory", "logs"] # Security Whitelist

    def create(self, table, data):
        if table not in self.allowed_tables: return "Table Restricted"
        session = self.manager.get_neon_session()
        try:
            cols = ", ".join(data.keys())
            placeholders = ", ".join([f":{k}" for k in data.keys()])
            query = text(f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) RETURNING id")
            result = session.execute(query, data)
            session.commit()
            return result.fetchone()[0]
        except Exception as e:
            session.rollback()  
            logging.error(f"Neon Create Error: {e}")
            raise e             
        finally: 
            session.close()

    def read(self, table, item_id):
        if table not in self.allowed_tables: return None # ADDED SECURITY
        
        session = self.manager.get_neon_session()
        try:
            query = text(f"SELECT * FROM {table} WHERE id = :id")
            result = session.execute(query, {"id": item_id}).fetchone()
            return dict(result._mapping) if result else None
        except Exception as e:
            logging.error(f"Neon Read Error: {e}")
            return None
        finally: 
            session.close()

    def update(self, table, item_id, data):
        if table not in self.allowed_tables: return False # ADDED SECURITY
        
        clean_params = {k: v for k, v in data.items() if k != 'id'}
        if not clean_params: return False
            
        session = self.manager.get_neon_session()
        try:
            set_clause = ", ".join([f"{k} = :{k}" for k in clean_params.keys()])
            query = text(f"UPDATE {table} SET {set_clause} WHERE id = :id")
            clean_params['id'] = item_id
            
            session.execute(query, clean_params)
            session.commit()
            return "Updated"
        except Exception as e:
            session.rollback()
            logging.error(f"Neon Update Error: {e}")
            return "Update Failed"
        finally: 
            session.close()

    def delete(self, table, item_id):
        if table not in self.allowed_tables: return False # ADDED SECURITY
        
        session = self.manager.get_neon_session()
        try:
            query = text(f"DELETE FROM {table} WHERE id = :id")
            session.execute(query, {"id": item_id})
            session.commit()
            return "Deleted"
        except Exception as e:
            session.rollback()
            logging.error(f"Neon Delete Error: {e}")
            return "Delete Failed"
        finally: 
            session.close()