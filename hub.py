import logging
from typing import Dict, Any
from .discovery.data_discovery import DataDiscovery
from .connectors.connector_factory import ConnectorFactory
from .processing.processing_pipeline import ProcessingPipeline
from .storage.storage_manager import StorageManager

class Hub:
    """The main class managing the Autonomous Data Integration Hub."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Initialize submodules
        self.data_discovery = DataDiscovery()
        self.connector_factory = ConnectorFactory()
        self.processing_pipeline = ProcessingPipeline()
        self.storage_manager = StorageManager()
        
    def discover_sources(self) -> Dict[str, Any]:
        """Discover available data sources."""
        try:
            discovered = self.data_discovery.scan()
            self.logger.info("Discovered %d sources.", len(discovered))
            return discovered
        except Exception as e:
            self.logger.error("Failed to discover sources: %s", str(e))
            raise
    
    def connect(self, source_id: str) -> Any:
        """Connect to a data source using its ID."""
        try:
            connector = self.connector_factory.create_connector(source_id)
            return connector.connect()
        except Exception as e:
            self.logger.error("Connection failed for source %s: %s", source_id, str(e))
            raise
    
    def process(self, raw_data: Any) -> Dict[str, Any]:
        """Process raw data through the pipeline."""
        try:
            processed = self.processing_pipeline.run(raw_data)
            return processed
        except Exception as e:
            self.logger.error("Processing failed: %s", str(e))
            raise
    
    def store(self, data: Any, destination: str) -> bool:
        """Store processed data in the specified destination."""
        try:
            result = self.storage_manager.store(data, destination)
            self.logger.info("Data stored successfully.")
            return result
        except Exception as e:
            self.logger.error("Storage failed: %s", str(e))
            raise
    
    def run_pipeline(self, source_id: str, destination: str) -> bool:
        """Run the entire pipeline from discovery to storage."""
        try:
            # Discover sources
            sources = self.discover_sources()
            if not sources:
                raise ValueError("No data sources found.")
            
            # Connect and fetch data
            data = self.connect(source_id)
            
            # Process data
            processed_data = self.process(data)
            
            # Store data
            self.store(processed_data, destination)
            
            return True
        except Exception as e:
            self.logger.error("Pipeline execution failed: %s", str(e))
            raise