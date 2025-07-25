#!/usr/bin/env python3
"""
Wikipedia dump downloader for Chinese Wikipedia 20250401 dump.
"""

import os
import requests
from tqdm import tqdm
import sys


class WikiDumpDownloader:
    def __init__(self):
        self.base_url = "https://dumps.wikimedia.org/zhwiki/20250401/"
        self.dump_files = [
            "zhwiki-20250401-pages-articles.xml.bz2"
        ]
        
    def download_file(self, filename, output_dir="data"):
        """Download a single dump file with progress bar."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        url = self.base_url + filename
        output_path = os.path.join(output_dir, filename)
        
        # Check if file already exists
        if os.path.exists(output_path):
            print(f"File {filename} already exists, skipping download.")
            return output_path
            
        print(f"Downloading {filename}...")
        
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(output_path, 'wb') as f, tqdm(
                desc=filename,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
                        
            print(f"Downloaded {filename} successfully!")
            return output_path
            
        except requests.RequestException as e:
            print(f"Error downloading {filename}: {e}")
            return None
    
    def download_all(self, output_dir="data"):
        """Download all required dump files."""
        downloaded_files = []
        for filename in self.dump_files:
            result = self.download_file(filename, output_dir)
            if result:
                downloaded_files.append(result)
        return downloaded_files


if __name__ == "__main__":
    downloader = WikiDumpDownloader()
    files = downloader.download_all()
    print(f"Downloaded {len(files)} files: {files}")