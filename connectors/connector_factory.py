from typing import Any
from .connectors.csv_connector import CSVConnector
from .connectors.sql_connector import SQLConnector

class ConnectorFactory:
    """Creates and manages connectors for various data sources."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def create_connector(self, source_id: str) -> Any:
        """Create a connector based on the source ID."""
        try:
            if source_id.endswith("_csv"):
                return CSVConnector()
            elif source_id.endswith("_sql"):
                return SQLConnector()
            else:
                raise ValueError("Unknown source type.")
        except Exception as e:
            self.logger.error("Failed to create connector for %s: %s", source_id, str(e))
            raise