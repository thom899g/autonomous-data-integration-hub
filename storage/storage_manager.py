from typing import Any
import json

class StorageManager:
    """Manages the storage of processed data."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def store(self, data: Any, destination: str) -> bool:
        """Store data in the specified destination."""
        try:
            if destination.endswith(".json"):
                with open(destination, "w") as f:
                    json.dump(data, f)
            else:
                raise ValueError("Unsupported destination type.")
                
            return True
        except Exception as e:
            self.logger.error("Storage failed: %s", str(e))
            raise