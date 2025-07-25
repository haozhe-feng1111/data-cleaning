#!/usr/bin/env python3
"""
Main script for Wikipedia data cleaning project.
Process Chinese Wikipedia dump and convert to JSONL format.
"""

import argparse
import os
import sys
from typing import Optional
from tqdm import tqdm

from downloader import WikiDumpDownloader
from parser import WikiXMLParser
from cleaner import TextCleaner
from formatter import JSONLFormatter


class WikipediaProcessor:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        self.downloader = WikiDumpDownloader()
        self.cleaner = TextCleaner()
        self.formatter = JSONLFormatter(os.path.join(output_dir, "cleaned_wikipedia.jsonl"))
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def download_data(self, data_dir: str = "data") -> Optional[str]:
        """Download Wikipedia dump files."""
        print("=== DOWNLOADING WIKIPEDIA DUMP ===")
        files = self.downloader.download_all(data_dir)
        
        if not files:
            print("Error: No files were downloaded successfully.")
            return None
            
        dump_file = files[0]  # Should be the articles file
        print(f"Downloaded dump file: {dump_file}")
        return dump_file
    
    def process_dump(self, dump_file: str, max_articles: Optional[int] = None, 
                    progress_interval: int = 1000) -> int:
        """
        Process the Wikipedia dump file.
        
        Args:
            dump_file: Path to the compressed XML dump file
            max_articles: Maximum number of articles to process (None for all)
            progress_interval: How often to report progress
            
        Returns:
            Number of articles successfully processed
        """
        print("=== PROCESSING WIKIPEDIA DUMP ===")
        print(f"Input file: {dump_file}")
        print(f"Output file: {self.formatter.output_file}")
        
        if max_articles:
            print(f"Processing maximum {max_articles:,} articles")
        else:
            print("Processing all articles")
        
        parser = WikiXMLParser(dump_file)
        processed_count = 0
        written_count = 0
        
        # Create progress bar
        pbar = tqdm(desc="Processing articles", unit="articles")
        
        try:
            for page_data in parser.parse_dump():
                processed_count += 1
                pbar.update(1)
                
                # Extract basic page information
                title = page_data.get('title', '')
                text = page_data.get('text', '')
                page_id = page_data.get('id', '')
                timestamp = page_data.get('timestamp', '')
                
                # Clean the article
                cleaned_article = self.cleaner.clean_article(
                    title=title,
                    text=text,
                    page_id=page_id,
                    timestamp=timestamp
                )
                
                # Write to output if cleaning was successful
                if cleaned_article:
                    if self.formatter.write_record(cleaned_article):
                        written_count += 1
                
                # Update progress description
                if processed_count % progress_interval == 0:
                    pbar.set_description(
                        f"Processed: {processed_count:,}, Written: {written_count:,}"
                    )
                
                # Check if we've reached the maximum
                if max_articles and written_count >= max_articles:
                    print(f"\\nReached maximum of {max_articles:,} articles")
                    break
                    
        except KeyboardInterrupt:
            print("\\nProcessing interrupted by user")
        except Exception as e:
            print(f"\\nError during processing: {e}")
        finally:
            pbar.close()
        
        print(f"\\nProcessing completed:")
        print(f"- Processed: {processed_count:,} articles")
        print(f"- Written: {written_count:,} articles")
        print(f"- Success rate: {written_count/max(processed_count, 1)*100:.1f}%")
        
        return written_count
    
    def create_sample(self, sample_size: int = 1000) -> str:
        """Create a sample file with specified number of records."""
        print(f"=== CREATING SAMPLE ({sample_size:,} records) ===")
        
        input_file = self.formatter.output_file
        sample_file = os.path.join(self.output_dir, f"sample_{sample_size}.jsonl")
        
        if not os.path.exists(input_file):
            print(f"Error: Input file does not exist: {input_file}")
            return ""
        
        actual_count = self.formatter.create_sample_file(
            input_file=input_file,
            sample_size=sample_size,
            output_file=sample_file
        )
        
        print(f"Created sample file with {actual_count:,} records: {sample_file}")
        return sample_file
    
    def validate_output(self) -> bool:
        """Validate the output files."""
        print("=== VALIDATING OUTPUT ===")
        
        # Validate main output file
        main_validation = self.formatter.validate_output_file()
        print(f"Main file validation: {'PASSED' if main_validation['valid'] else 'FAILED'}")
        
        if not main_validation['valid']:
            print("Validation errors:")
            for error in main_validation.get('errors', [])[:5]:  # Show first 5 errors
                print(f"  - {error}")
        else:
            print(f"  - Total records: {main_validation['valid_records']:,}")
            print(f"  - Invalid records: {main_validation['invalid_records']:,}")
        
        return main_validation['valid']
    
    def print_final_stats(self):
        """Print final processing statistics."""
        self.formatter.print_stats()


def main():
    parser = argparse.ArgumentParser(
        description="Process Chinese Wikipedia dump to JSONL format"
    )
    
    parser.add_argument(
        '--download', 
        action='store_true', 
        help="Download Wikipedia dump files"
    )
    
    parser.add_argument(
        '--process', 
        type=str, 
        metavar='DUMP_FILE',
        help="Process Wikipedia dump file"
    )
    
    parser.add_argument(
        '--max-articles', 
        type=int, 
        help="Maximum number of articles to process"
    )
    
    parser.add_argument(
        '--sample', 
        type=int, 
        default=1000,
        help="Create sample file with N records (default: 1000)"
    )
    
    parser.add_argument(
        '--output-dir', 
        type=str, 
        default="output",
        help="Output directory (default: output)"
    )
    
    parser.add_argument(
        '--data-dir', 
        type=str, 
        default="data",
        help="Data directory for downloads (default: data)"
    )
    
    args = parser.parse_args()
    
    # Create processor
    processor = WikipediaProcessor(output_dir=args.output_dir)
    
    # Handle download
    if args.download:
        dump_file = processor.download_data(args.data_dir)
        if not dump_file:
            sys.exit(1)
        
        # If no specific processing file was given, use the downloaded file
        if not args.process:
            args.process = dump_file
    
    # Handle processing
    if args.process:
        if not os.path.exists(args.process):
            print(f"Error: Dump file does not exist: {args.process}")
            sys.exit(1)
        
        written_count = processor.process_dump(args.process, args.max_articles)
        
        if written_count > 0:
            # Validate output
            processor.validate_output()
            
            # Create sample
            processor.create_sample(args.sample)
            
            # Print final statistics
            processor.print_final_stats()
        else:
            print("No articles were processed successfully.")
            sys.exit(1)
    
    # If neither download nor process was specified, show help
    if not args.download and not args.process:
        parser.print_help()
        print("\\nExamples:")
        print("  python main.py --download")
        print("  python main.py --process data/zhwiki-20250401-pages-articles.xml.bz2")
        print("  python main.py --download --max-articles 5000 --sample 1000")


if __name__ == "__main__":
    main()