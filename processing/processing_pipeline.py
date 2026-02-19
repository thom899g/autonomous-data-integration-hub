from typing import Dict, Any
from .processors.null_processor import NullProcessor

class ProcessingPipeline:
    """Manages the data processing pipeline."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.processors = [NullProcessor()]
        
    def run(self, raw_data: Any) -> Dict[str, Any]:
        """Run the processing pipeline on raw data."""
        try:
            processed_data = raw_data.copy()
            
            for processor in self.processors:
                processed_data = processor.process(processed_data)
                
            return {"processed": processed_data}
        except Exception as e:
            self.logger.error("Processing failed: %s", str(e))
            raise