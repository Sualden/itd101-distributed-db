from decimal import Decimal

class DynamoCRUD:
    def __init__(self, manager):
        self.manager = manager

    def _to_decimal(self, obj):
        """
        A robust recursive function that guarantees every float 
        is safely converted to a Decimal for AWS DynamoDB.
        """
        if isinstance(obj, list):
            return [self._to_decimal(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: self._to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, float):
            return Decimal(str(obj))
        return obj

    def create(self, table, data):
        resource = self.manager.get_dynamo_resource()
        dynamo_table = resource.Table(table)
        
        # Safely convert data to remove floats BEFORE sending to AWS
        safe_data = self._to_decimal(data)
        
        dynamo_table.put_item(Item=safe_data)
        return safe_data.get("id")

    def read(self, table, query_key):
        resource = self.manager.get_dynamo_resource()
        dynamo_table = resource.Table(table)
        
        response = dynamo_table.get_item(Key=query_key)
        return response.get("Item")

    def update(self, table, query_key, data):
        resource = self.manager.get_dynamo_resource()
        dynamo_table = resource.Table(table)
        
        safe_data = self._to_decimal(data)
        
        update_expr = "SET " + ", ".join(f"#{k} = :{k}" for k in safe_data.keys())
        expr_names = {f"#{k}": k for k in safe_data.keys()}
        expr_values = {f":{k}": v for k, v in safe_data.items()}
        
        dynamo_table.update_item(
            Key=query_key,
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values
        )
        return True

    def delete(self, table, query_key):
        resource = self.manager.get_dynamo_resource()
        dynamo_table = resource.Table(table)
        
        dynamo_table.delete_item(Key=query_key)
        return True