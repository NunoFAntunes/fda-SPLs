"""
Environment variable loader for database configuration.
Loads variables from .env file if available.
"""

import os
from pathlib import Path
from typing import Dict, Any


def load_env_file(env_file: str = ".env") -> Dict[str, str]:
    """Load environment variables from .env file."""
    env_vars = {}
    
    # Try to find .env file in current directory or project root
    env_paths = [
        Path(env_file),
        Path.cwd() / env_file,
        Path(__file__).parent.parent.parent / env_file,  # Go up to project root
    ]
    
    env_path = None
    for path in env_paths:
        if path.exists():
            env_path = path
            break
    
    if env_path and env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        # Remove quotes if present
                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        
                        env_vars[key] = value
                        # Also set in os.environ if not already set
                        if key not in os.environ:
                            os.environ[key] = value
    
    return env_vars


# Load .env file automatically when module is imported
_loaded_vars = load_env_file()
if _loaded_vars:
    print(f"Loaded {len(_loaded_vars)} environment variables from .env file")
else:
    print("No .env file found, using system environment variables")