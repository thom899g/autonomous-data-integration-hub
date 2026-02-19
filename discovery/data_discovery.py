from typing import Dict, Any
import os

class DataDiscovery:
    """Handles discovery of available data sources."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def scan(self) -> Dict[str, Any]:
        """Scan for available data sources and return their metadata."""
        try:
            sources = []
            
            # Check files
            if os.path.exists("data_files"):
                files = os.listdir("data_files")
                sources.extend([(f, "file", {"path": f}) for f in files])
                
            # Check databases
            if os.path.exists("databases"):
                dbs = ["db1", "db2"]  # Assume these exist
                sources.extend([(d, "database", {"name": d}) for d in dbs])
            
            self.logger.info("Found %d sources.", len(sources))
            return {"sources": sources}
        except Exception as e:
            self.logger.error("Data discovery failed: %s", str(e))
            raise