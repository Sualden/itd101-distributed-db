from sqlalchemy import text
import json
import logging

class AivenCRUD:
    def __init__(self, manager):
        self.manager = manager
        self.allowed_tables = ["users", "inventory", "orders"]
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Automatically creates required tables if they are missing in Aiven."""
        session = self.manager.get_aiven_session()
        try:
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(255) UNIQUE,
                    email VARCHAR(255)
                );
            """))
            
            session.commit()
            logging.info("Aiven Database: Tables verified/created successfully.")
        except Exception as e:
            session.rollback()
            logging.error(f"Aiven Table Setup Error: {e}")
        finally:
            session.close()

    def create(self, table, data):
        if table not in self.allowed_tables: return "Access Denied"
        session = self.manager.get_aiven_session()
        try:
            columns = ", ".join(data.keys())
            placeholders = ", ".join([f":{k}" for k in data.keys()])
            
            query = text(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})")
            session.execute(query, data)
            res = session.execute(text("SELECT LAST_INSERT_ID()")).fetchone()
            session.commit()
            return res[0] if res else None
        except Exception as e:
            session.rollback()  
            logging.error(f"Aiven Create Error: {e}")
            raise e
        finally: 
            session.close()

    def read(self, table, target_id):
        if table not in self.allowed_tables: return None
        session = self.manager.get_aiven_session()
        try:
            query = text(f"SELECT * FROM {table} WHERE id = :target_id")
            result = session.execute(query, {"target_id": target_id}).fetchone()
            return dict(result._mapping) if result else None
        except Exception as e:
            logging.error(f"Aiven Read Error: {e}")
            return None
        finally: 
            session.close()

    def update(self, table, target_id, data):
        if table not in self.allowed_tables: return False
        
        clean_params = {k: v for k, v in data.items() if k != 'id'}
        if not clean_params:
            return False 
            
        session = self.manager.get_aiven_session()
        try:
            set_clause = ", ".join([f"{k} = :{k}" for k in clean_params.keys()])
            
            query = text(f"UPDATE {table} SET {set_clause} WHERE id = :target_id")
            clean_params['target_id'] = target_id
            
            session.execute(query, clean_params)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logging.error(f"Aiven Update Error: {e}")
            return False
        finally: 
            session.close()

    def delete(self, table, target_id):

        if table not in self.allowed_tables: return False
        
        session = self.manager.get_aiven_session()
        try:
            query = text(f"DELETE FROM {table} WHERE id = :target_id")
            session.execute(query, {"target_id": target_id})
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            logging.error(f"Aiven Delete Error: {e}")
            return False
        finally: 
            session.close()

    def read_all(self, table):
        if table not in self.allowed_tables: return []
        session = self.manager.get_aiven_session()
        try:
            query = text(f"SELECT * FROM {table}")
            result = session.execute(query).fetchall()
            return [dict(row._mapping) for row in result]
        except Exception as e:
            logging.error(f"Aiven Read All Error: {e}")
            return []
        finally: 
            session.close()