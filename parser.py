#!/usr/bin/env python3
"""
Wikipedia XML dump parser for extracting and processing articles.
"""

import xml.etree.ElementTree as ET
import bz2
import re
from typing import Iterator, Dict, Any
import mwparserfromhell


class WikiXMLParser:
    def __init__(self, dump_file: str):
        self.dump_file = dump_file
        self.namespace_pattern = re.compile(r'^([^:]+):')
        
    def parse_dump(self) -> Iterator[Dict[str, Any]]:
        """Parse Wikipedia XML dump and yield article data."""
        
        def get_text(element):
            return element.text if element is not None else ""
        
        # Open bz2 compressed file
        with bz2.open(self.dump_file, 'rt', encoding='utf-8') as f:
            # Use iterparse for memory efficiency
            context = ET.iterparse(f, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)
            
            current_page = {}
            current_element = None
            
            for event, elem in context:
                tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                
                if event == 'start':
                    current_element = tag
                elif event == 'end':
                    if tag == 'page':
                        # Process complete page
                        if self._should_include_page(current_page):
                            yield current_page
                        current_page = {}
                        elem.clear()
                    elif tag in ['title', 'id', 'text', 'timestamp']:
                        if tag == 'id' and 'id' not in current_page:
                            # Only take the first id (page id, not revision id)
                            current_page[tag] = get_text(elem)
                        elif tag != 'id':
                            current_page[tag] = get_text(elem)
                    
                    # Clear processed elements to save memory
                    if tag != 'page':
                        elem.clear()
    
    def _should_include_page(self, page_data: Dict[str, Any]) -> bool:
        """Determine if a page should be included in the output."""
        title = page_data.get('title', '')
        text = page_data.get('text', '')
        
        # Skip if no title or text
        if not title or not text:
            return False
            
        # Skip redirects
        if text.strip().startswith('#重定向') or text.strip().startswith('#REDIRECT'):
            return False
            
        # Skip disambiguation pages
        if '消歧义' in title or 'disambiguation' in title.lower():
            return False
            
        # Skip pages in certain namespaces
        namespace_match = self.namespace_pattern.match(title)
        if namespace_match:
            namespace = namespace_match.group(1)
            skip_namespaces = {
                'Wikipedia', '维基百科', 'User', '用户', 'Talk', '讨论',
                'File', '文件', 'MediaWiki', 'Template', '模板',
                'Help', '帮助', 'Category', '分类', 'Portal', '主题'
            }
            if namespace in skip_namespaces:
                return False
        
        # Skip short articles (less than 200 characters of text)
        if len(text.strip()) < 200:
            return False
            
        return True
    
    def extract_text_content(self, wiki_markup: str) -> str:
        """Extract plain text from Wikipedia markup."""
        try:
            # Parse the wikitext
            wikicode = mwparserfromhell.parse(wiki_markup)
            
            # Remove templates, links, and other markup
            text = wikicode.strip_code()
            
            # Additional cleaning
            text = self._clean_text(text)
            
            return text.strip()
            
        except Exception as e:
            # Fallback to basic cleaning if mwparserfromhell fails
            return self._basic_text_cleaning(wiki_markup)
    
    def _clean_text(self, text: str) -> str:
        """Apply additional text cleaning rules."""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove citation markers like [1], [2], etc.
        text = re.sub(r'\[\d+\]', '', text)
        
        # Remove double brackets
        text = re.sub(r'\[\[([^|\]]+)(\|[^\]]+)?\]\]', r'\1', text)
        
        # Remove curly braces content
        text = re.sub(r'\{\{[^}]+\}\}', '', text)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove lines that are mostly markup
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if len(line) > 10 and not re.match(r'^[|\s\{\}=<>]+$', line):
                cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def _basic_text_cleaning(self, text: str) -> str:
        """Basic text cleaning as fallback."""
        # Remove common wiki markup
        text = re.sub(r'\{\{[^}]*\}\}', '', text)
        text = re.sub(r'\[\[([^|\]]+)(\|[^\]]+)?\]\]', r'\1', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\[\d+\]', '', text)
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()


if __name__ == "__main__":
    # Test the parser
    import sys
    if len(sys.argv) != 2:
        print("Usage: python parser.py <dump_file.xml.bz2>")
        sys.exit(1)
    
    parser = WikiXMLParser(sys.argv[1])
    count = 0
    for page in parser.parse_dump():
        print(f"Title: {page['title']}")
        text = parser.extract_text_content(page.get('text', ''))
        print(f"Text length: {len(text)}")
        print("---")
        count += 1
        if count >= 5:  # Show first 5 pages for testing
            break