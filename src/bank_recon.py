import json
import logging
import subprocess
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union, Any
from config import predefined_config, predefined_metrics
from pandas import DataFrame, json_normalize



class BankRecon:
    def __init__(self, files: List[str], schema: Dict, custom_config: Optional[Dict] = None):
        self.files = files
        self.canon_files = []
        self.schema = schema
        self.dataframes = []

        if not custom_config:
            self.config = predefined_config
        else:
            self.config.update(custom_config)

        logging.basicConfig(filename=self.config['log_file'], level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('reconciliation')
        self.logger.info(f"Starting reconciliation with files: {', '.join(files)}")

        self.metrics = predefined_metrics

    def jq_canonicalize(self) -> None:
        self.logger.info("Starting canonicalization")

        for file in self.files:
            try:
                out = file.replace(".json", "_canon.json")
                # Security improvement: avoid shell=True
                subprocess.run(
                    ["jq", "--sort-keys", ".", file], 
                    stdout=open(out, 'w'),
                    check=True
                )
                self.canon_files.append(out)
                self.logger.info(f"Canonicalized {file} to {out}")
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error canonicalizing {file}: {str(e)}")
                raise RuntimeError(f"Failed to canononicalize {file}") from e
            except Exception as e:
                self.logger.error(f"Unexpected error with {file}: {str(e)}")
                raise

    def validate_with_schema(self) -> None:
        return

    def load_and_flatten(self, path: str) -> DataFrame:
        return 