#!/usr/bin/env python3
"""
JSONL output formatter for processed Wikipedia data.
"""

import json
import os
from typing import Dict, Any, Iterator, Optional
from datetime import datetime


class JSONLFormatter:
    def __init__(self, output_file: str = "output/cleaned_wikipedia.jsonl"):
        self.output_file = output_file
        self.ensure_output_dir()
        self.stats = {
            "total_processed": 0,
            "total_written": 0,
            "total_skipped": 0,
            "start_time": datetime.now().isoformat()
        }
    
    def ensure_output_dir(self):
        """Ensure output directory exists."""
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
    
    def write_record(self, record: Dict[str, Any]) -> bool:
        """
        Write a single record to the JSONL file.
        
        Args:
            record: Dictionary containing 'text' and 'meta' keys
            
        Returns:
            True if written successfully, False otherwise
        """
        try:
            # Validate record format
            if not self._validate_record(record):
                self.stats["total_skipped"] += 1
                return False
            
            # Write to file
            with open(self.output_file, 'a', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, separators=(',', ':'))
                f.write('\n')
            
            self.stats["total_written"] += 1
            return True
            
        except Exception as e:
            print(f"Error writing record: {e}")
            self.stats["total_skipped"] += 1
            return False
    
    def write_batch(self, records: Iterator[Dict[str, Any]], max_records: Optional[int] = None) -> int:
        """
        Write multiple records to the JSONL file.
        
        Args:
            records: Iterator of record dictionaries
            max_records: Maximum number of records to write (None for unlimited)
            
        Returns:
            Number of records successfully written
        """
        written_count = 0
        
        for record in records:
            self.stats["total_processed"] += 1
            
            if self.write_record(record):
                written_count += 1
                
            # Check if we've reached the maximum
            if max_records and written_count >= max_records:
                break
                
            # Progress reporting
            if self.stats["total_processed"] % 1000 == 0:
                print(f"Processed {self.stats['total_processed']} records, "
                      f"written {self.stats['total_written']}, "
                      f"skipped {self.stats['total_skipped']}")
        
        return written_count
    
    def _validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate that a record has the correct format."""
        # Check required keys
        if 'text' not in record or 'meta' not in record:
            return False
            
        # Check data types
        if not isinstance(record['text'], str) or not isinstance(record['meta'], dict):
            return False
            
        # Check minimum content requirements
        if len(record['text'].strip()) < 50:
            return False
            
        # Validate metadata
        meta = record['meta']
        if 'title' not in meta or not isinstance(meta['title'], str):
            return False
            
        return True
    
    def create_sample_file(self, input_file: str, sample_size: int = 1000, 
                          output_file: str = "output/sample_1000.jsonl") -> int:
        """
        Create a sample file with specified number of records.
        
        Args:
            input_file: Path to the full JSONL file
            sample_size: Number of records to sample
            output_file: Output file for the sample
            
        Returns:
            Number of records in the sample file
        """
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        written_count = 0
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile):
                if written_count >= sample_size:
                    break
                    
                try:
                    record = json.loads(line.strip())
                    if self._validate_record(record):
                        json.dump(record, outfile, ensure_ascii=False, separators=(',', ':'))
                        outfile.write('\n')
                        written_count += 1
                except json.JSONDecodeError:
                    print(f"Invalid JSON on line {line_num + 1}")
                    continue
        
        print(f"Created sample file with {written_count} records: {output_file}")
        return written_count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        current_stats = self.stats.copy()
        current_stats["end_time"] = datetime.now().isoformat()
        
        # Calculate processing rate
        start_time = datetime.fromisoformat(self.stats["start_time"])
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if duration > 0:
            current_stats["records_per_second"] = self.stats["total_processed"] / duration
        else:
            current_stats["records_per_second"] = 0
            
        return current_stats
    
    def print_stats(self):
        """Print processing statistics."""
        stats = self.get_stats()
        print("\n" + "="*50)
        print("PROCESSING STATISTICS")
        print("="*50)
        print(f"Total processed: {stats['total_processed']:,}")
        print(f"Total written: {stats['total_written']:,}")
        print(f"Total skipped: {stats['total_skipped']:,}")
        print(f"Success rate: {stats['total_written']/max(stats['total_processed'], 1)*100:.1f}%")
        print(f"Processing rate: {stats['records_per_second']:.1f} records/second")
        print(f"Output file: {self.output_file}")
        
        # File size information
        if os.path.exists(self.output_file):
            file_size = os.path.getsize(self.output_file)
            print(f"Output file size: {file_size / (1024*1024):.1f} MB")
        
        print("="*50)
    
    def validate_output_file(self, filename: str = None) -> Dict[str, Any]:
        """
        Validate the output JSONL file.
        
        Args:
            filename: File to validate (defaults to self.output_file)
            
        Returns:
            Dictionary with validation results
        """
        if filename is None:
            filename = self.output_file
            
        if not os.path.exists(filename):
            return {"valid": False, "error": "File does not exist"}
        
        validation_stats = {
            "valid": True,
            "total_lines": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "errors": []
        }
        
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                validation_stats["total_lines"] += 1
                
                try:
                    record = json.loads(line.strip())
                    if self._validate_record(record):
                        validation_stats["valid_records"] += 1
                    else:
                        validation_stats["invalid_records"] += 1
                        validation_stats["errors"].append(f"Line {line_num}: Invalid record format")
                except json.JSONDecodeError as e:
                    validation_stats["invalid_records"] += 1
                    validation_stats["errors"].append(f"Line {line_num}: JSON decode error - {e}")
                
                # Limit error reporting
                if len(validation_stats["errors"]) >= 10:
                    validation_stats["errors"].append("... (truncated, too many errors)")
                    break
        
        if validation_stats["invalid_records"] > 0:
            validation_stats["valid"] = False
        
        return validation_stats


def create_example_records() -> list:
    """Create example records for testing."""
    examples = [
        {
            "text": "中华人民共和国，简称中国，是位于东亚的社会主义国家。中国是世界上人口最多的国家，拥有约14亿人口。中国的首都是北京，最大的城市是上海。中国有着五千年的悠久历史和灿烂的文化。",
            "meta": {
                "title": "中华人民共和国",
                "page_id": "12345",
                "char_count": 1250,
                "source": "zh_wikipedia_20250201",
                "categories": ["国家", "亚洲"],
                "timestamp": "2025-02-01T00:00:00Z"
            }
        },
        {
            "text": "北京是中华人民共和国的首都，也是全国的政治、文化中心。北京位于华北平原北部，是一座有着三千多年历史的古城。北京有许多著名的历史古迹，如紫禁城、天坛、长城等。",
            "meta": {
                "title": "北京",
                "page_id": "67890",
                "char_count": 890,
                "source": "zh_wikipedia_20250201",
                "categories": ["城市", "中国首都"],
                "timestamp": "2025-02-01T00:00:00Z"
            }
        }
    ]
    return examples


if __name__ == "__main__":
    # Test the formatter
    formatter = JSONLFormatter("test_output.jsonl")
    
    # Create and write example records
    examples = create_example_records()
    
    print("Writing example records...")
    for record in examples:
        formatter.write_record(record)
    
    # Print statistics
    formatter.print_stats()
    
    # Validate the output
    validation_result = formatter.validate_output_file("test_output.jsonl")
    print(f"\nValidation result: {validation_result}")
    
    # Create sample file
    if validation_result["valid"]:
        formatter.create_sample_file("test_output.jsonl", 2, "test_sample.jsonl")