#!/usr/bin/env python3
"""
Text cleaning and processing utilities for Wikipedia content.
"""

import re
import regex
from typing import Dict, Any, List


class TextCleaner:
    def __init__(self):
        # Compile regex patterns for efficiency
        self.patterns = self._compile_patterns()
        
    def _compile_patterns(self) -> Dict[str, regex.Pattern]:
        """Compile all regex patterns used for cleaning."""
        return {
            # Remove URLs
            'urls': regex.compile(r'https?://\S+', regex.IGNORECASE),
            
            # Remove email addresses
            'emails': regex.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            
            # Remove Wikipedia-specific markup
            'wiki_refs': regex.compile(r'<ref[^>]*>.*?</ref>', regex.DOTALL | regex.IGNORECASE),
            'wiki_comments': regex.compile(r'<!--.*?-->', regex.DOTALL),
            'wiki_categories': regex.compile(r'\[\[Category:[^\]]+\]\]', regex.IGNORECASE),
            'wiki_files': regex.compile(r'\[\[File:[^\]]+\]\]', regex.IGNORECASE),
            'wiki_images': regex.compile(r'\[\[Image:[^\]]+\]\]', regex.IGNORECASE),
            
            # Remove infoboxes and templates
            'infobox': regex.compile(r'\{\{Infobox[^}]*\}\}', regex.DOTALL | regex.IGNORECASE),
            'templates': regex.compile(r'\{\{[^}]*\}\}', regex.DOTALL),
            
            # Remove table markup
            'tables': regex.compile(r'\{\|.*?\|\}', regex.DOTALL),
            
            # Clean up brackets and parentheses with only numbers/punctuation
            'empty_brackets': regex.compile(r'\([^a-zA-Z\u4e00-\u9fff]*\)'),
            'citation_brackets': regex.compile(r'\[\s*\d+\s*\]'),
            
            # Remove excessive whitespace
            'multiple_spaces': regex.compile(r' {2,}'),
            'multiple_newlines': regex.compile(r'\n{3,}'),
            
            # Remove lines that are mostly punctuation or formatting
            'punctuation_lines': regex.compile(r'^[^\w\u4e00-\u9fff]*$', regex.MULTILINE),
            
            # Remove common Wikipedia navigation elements
            'navigation': regex.compile(r'(上一页|下一页|返回|目录|导航|分类|Category:|Template:)', regex.IGNORECASE),
        }
    
    def clean_article(self, title: str, text: str, page_id: str = None, timestamp: str = None) -> Dict[str, Any]:
        """
        Clean a Wikipedia article and return structured data.
        
        Args:
            title: Article title
            text: Raw Wikipedia markup text
            page_id: Wikipedia page ID
            timestamp: Article timestamp
            
        Returns:
            Dictionary with cleaned text and metadata
        """
        # Extract and clean the main text content
        cleaned_text = self._clean_text_content(text)
        
        # Skip if text is too short after cleaning
        if len(cleaned_text.strip()) < 100:
            return None
            
        # Extract metadata
        meta = self._extract_metadata(title, text, page_id, timestamp)
        
        # Final quality checks
        if not self._passes_quality_checks(cleaned_text, meta):
            return None
        
        return {
            "text": cleaned_text,
            "meta": meta
        }
    
    def _clean_text_content(self, text: str) -> str:
        """Apply comprehensive text cleaning."""
        # Remove Wikipedia-specific markup
        text = self.patterns['wiki_refs'].sub('', text)
        text = self.patterns['wiki_comments'].sub('', text)
        text = self.patterns['wiki_categories'].sub('', text)
        text = self.patterns['wiki_files'].sub('', text)
        text = self.patterns['wiki_images'].sub('', text)
        
        # Remove templates and infoboxes
        text = self.patterns['infobox'].sub('', text)
        text = self.patterns['templates'].sub('', text)
        text = self.patterns['tables'].sub('', text)
        
        # Remove URLs and emails
        text = self.patterns['urls'].sub('', text)
        text = self.patterns['emails'].sub('', text)
        
        # Clean brackets and citations
        text = self.patterns['empty_brackets'].sub('', text)
        text = self.patterns['citation_brackets'].sub('', text)
        
        # Remove navigation elements
        text = self.patterns['navigation'].sub('', text)
        
        # Clean up whitespace
        text = self.patterns['multiple_spaces'].sub(' ', text)
        text = self.patterns['multiple_newlines'].sub('\n\n', text)
        
        # Remove lines that are mostly punctuation
        text = self.patterns['punctuation_lines'].sub('', text)
        
        # Split into paragraphs and clean each
        paragraphs = text.split('\n\n')
        cleaned_paragraphs = []
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if self._is_valid_paragraph(paragraph):
                cleaned_paragraphs.append(paragraph)
        
        return '\n\n'.join(cleaned_paragraphs)
    
    def _is_valid_paragraph(self, paragraph: str) -> bool:
        """Check if a paragraph should be kept."""
        if len(paragraph.strip()) < 20:
            return False
            
        # Check if paragraph has reasonable ratio of Chinese/English characters
        chinese_chars = len(regex.findall(r'[\u4e00-\u9fff]', paragraph))
        total_chars = len(regex.findall(r'[\w\u4e00-\u9fff]', paragraph))
        
        if total_chars < 10:
            return False
            
        # Should have at least some Chinese characters for Chinese Wikipedia
        if chinese_chars / total_chars < 0.3 and chinese_chars < 5:
            return False
            
        # Skip paragraphs that are mostly numbers or special characters
        if len(regex.findall(r'[^\w\u4e00-\u9fff\s]', paragraph)) / len(paragraph) > 0.5:
            return False
            
        return True
    
    def _extract_metadata(self, title: str, original_text: str, page_id: str = None, timestamp: str = None) -> Dict[str, Any]:
        """Extract metadata from the article."""
        meta = {
            "title": title,
            "char_count": len(original_text),
            "source": "zh_wikipedia_20250201"
        }
        
        if page_id:
            meta["page_id"] = page_id
            
        if timestamp:
            meta["timestamp"] = timestamp
            
        # Extract categories if present
        categories = regex.findall(r'\[\[Category:([^\]]+)\]\]', original_text, regex.IGNORECASE)
        if categories:
            meta["categories"] = [cat.strip() for cat in categories[:5]]  # Limit to 5 categories
            
        return meta
    
    def _passes_quality_checks(self, text: str, meta: Dict[str, Any]) -> bool:
        """Perform final quality checks on the cleaned text."""
        # Minimum length check
        if len(text.strip()) < 100:
            return False
            
        # Check for reasonable character distribution
        chinese_chars = len(regex.findall(r'[\u4e00-\u9fff]', text))
        if chinese_chars < 20:  # Should have at least 20 Chinese characters
            return False
            
        # Check that it's not just a list or table remnants
        lines = text.split('\n')
        non_empty_lines = [line.strip() for line in lines if line.strip()]
        
        if len(non_empty_lines) < 3:  # Should have at least 3 non-empty lines
            return False
            
        # Check for excessive repetition (spam detection)
        words = text.split()
        if len(set(words)) < len(words) * 0.5 and len(words) > 100:  # Less than 50% unique words
            return False
            
        return True


# Additional utility functions
def is_chinese_text(text: str, threshold: float = 0.5) -> bool:
    """Check if text is primarily Chinese."""
    chinese_chars = len(regex.findall(r'[\u4e00-\u9fff]', text))
    total_chars = len(regex.findall(r'[\w\u4e00-\u9fff]', text))
    
    if total_chars == 0:
        return False
        
    return chinese_chars / total_chars >= threshold


def extract_first_paragraph(text: str, max_length: int = 300) -> str:
    """Extract the first substantial paragraph as a summary."""
    paragraphs = text.split('\n\n')
    
    for paragraph in paragraphs:
        paragraph = paragraph.strip()
        if len(paragraph) >= 50 and is_chinese_text(paragraph):
            if len(paragraph) <= max_length:
                return paragraph
            else:
                # Truncate at sentence boundary
                sentences = regex.split(r'[。！？]', paragraph)
                result = ""
                for sentence in sentences:
                    if len(result + sentence) <= max_length:
                        result += sentence + "。"
                    else:
                        break
                return result.rstrip("。") + "。" if result else paragraph[:max_length]
    
    return ""


if __name__ == "__main__":
    # Test the cleaner
    cleaner = TextCleaner()
    
    test_text = """
    '''中国'''，全称为[[中华人民共和国]]<ref>参考资料</ref>，是一个位于东亚的国家。{{Infobox country}}
    
    == 历史 ==
    中国有着悠久的历史。[1][2]
    
    == 地理 ==
    中国幅员辽阔。https://example.com
    
    [[Category:国家]]
    """
    
    result = cleaner.clean_article("中国", test_text, "12345")
    if result:
        print("Cleaned text:")
        print(result["text"])
        print("\nMetadata:")
        print(result["meta"])
    else:
        print("Article did not pass quality checks")