"""Log template extraction using pattern matching."""
from typing import Dict, Optional
import re

class TemplateMiner:
    """Extract log templates using pattern matching.
    
    Extracts templates from log messages by replacing variable parts with wildcards.
    Maintains a mapping between templates and template IDs for consistent identification.
    """
    def __init__(self):
        """Initialize template miner."""
        self.templates = {}  # template_id -> template pattern
        self.template_to_id = {}  # template pattern -> template_id
        self.next_id = 1
        
    def extract(self, message: str) -> str:
        """Extract template ID for a message.
        
        Args:
            message: Log message to extract template from
            
        Returns:
            Template ID for the message
        """
        if not message:
            return ''
            
        # Generate template by replacing variables
        template = self._extract_template(message)
        
        # Get or create template ID
        if template not in self.template_to_id:
            template_id = f"T{self.next_id}"
            self.next_id += 1
            self.template_to_id[template] = template_id
            self.templates[template_id] = template
            
        return self.template_to_id[template]
        
    def _extract_template(self, message: str) -> str:
        """Extract template from message by replacing variables.
        
        Args:
            message: Log message to extract template from
            
        Returns:
            Template pattern with variables replaced by wildcards
        """
        # Replace block IDs
        template = re.sub(r'\bblk_\d+\b', 'blk_*', message)
        
        # Replace UUIDs
        template = re.sub(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', '*', template)
        
        # Replace IP addresses with ports
        template = re.sub(r'/?(\b(?:\d{1,3}\.){3}\d{1,3}\b)(?::(\d+))?', lambda m: '*' + (':*' if m.group(2) else ''), template)
        
        # Replace remaining numbers and hex
        template = re.sub(r'\b[0-9a-fA-F]{6,}\b', '*', template)
        template = re.sub(r'\b\d+\b', '*', template)
        
        # Clean up any remaining block references
        template = re.sub(r'\bblock \d+\b', 'block *', template)
        
        return template
        
    def get_template(self, template_id: str) -> Optional[str]:
        """Get template pattern for a template ID.
        
        Args:
            template_id: ID of template to retrieve
            
        Returns:
            Template pattern if found, None otherwise
        """
        return self.templates.get(template_id)
