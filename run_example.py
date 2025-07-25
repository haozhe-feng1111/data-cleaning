#!/usr/bin/env python3
"""
Example script to demonstrate the Wikipedia processing pipeline.
Creates sample data without downloading the full dump.
"""

import os
import json
from formatter import JSONLFormatter, create_example_records
from cleaner import TextCleaner


def create_sample_data():
    """Create sample Wikipedia data for demonstration."""
    
    # Sample Wikipedia articles (simplified)
    sample_articles = [
        {
            "title": "中华人民共和国",
            "id": "12345",
            "text": """'''中华人民共和国'''，简称'''中国'''，是位于[[东亚]]的[[社会主义国家]]。<ref>中华人民共和国宪法</ref>

== 历史 ==
中华人民共和国成立于1949年10月1日。{{Infobox country|name=中国}}

中国有着五千年的悠久历史。从古代的夏朝、商朝、周朝，到后来的秦汉、唐宋、明清，中国经历了漫长的历史发展过程。

== 地理 ==
中国位于亚洲东部，太平洋西岸。国土面积约960万平方公里，是世界第三大国家。

中国地势西高东低，山地、高原和丘陵约占陆地面积的67%，盆地和平原约占33%。

== 人口 ==
截至2020年，中国总人口约14.1亿人，是世界上人口最多的国家。

[[Category:国家]]
[[Category:亚洲国家]]""",
            "timestamp": "2025-02-01T00:00:00Z"
        },
        {
            "title": "北京市",
            "id": "67890", 
            "text": """'''北京市'''，简称'''京'''，是[[中华人民共和国]]的[[首都]]。<ref>中华人民共和国宪法</ref>

== 历史 ==
北京有着三千多年的建城史和八百多年的建都史。{{Infobox settlement}}

北京最初见于记载的名字为蓟。春秋战国时期，蓟国在此建都。秦朝设蓟县，为广阳郡治所。

== 地理 ==
北京位于华北平原北部，背靠燕山，有永定河流经老城西南。

北京的气候为典型的暖温带半湿润大陆性季风气候，夏季高温多雨，冬季寒冷干燥。

== 行政区划 ==
北京市下辖16个区，包括东城区、西城区、朝阳区等。

== 经济 ==
北京是中国的政治、文化、国际交往、科技创新中心。

[[Category:中国直辖市]]
[[Category:中国首都]]""",
            "timestamp": "2025-02-01T00:00:00Z"
        },
        {
            "title": "长城",
            "id": "11111",
            "text": """'''长城'''，又称'''万里长城'''，是中国古代的军事防御工程。<ref>中国历史文献</ref>

== 历史 ==
长城修筑的历史可上溯到西周时期。{{Infobox building}}

春秋战国时期，各国为了防御，都修筑了长城。秦始皇统一中国后，下令将各国长城连接起来。

== 建筑特点 ==
长城主要由城墙、敌楼、关城、烽火台等组成。

现存的长城遗迹主要为明朝所修建，西起嘉峪关，东至虎山长城。

== 文化意义 ==
长城是中华民族的象征，体现了中国古代人民的智慧和力量。

1987年，长城被联合国教科文组织列为世界文化遗产。

[[Category:中国长城]]
[[Category:世界文化遗产]]""",
            "timestamp": "2025-02-01T00:00:00Z"
        }
    ]
    
    return sample_articles


def process_sample_articles():
    """Process sample articles and create JSONL output."""
    
    print("=== CREATING SAMPLE WIKIPEDIA DATA ===")
    
    # Initialize components
    cleaner = TextCleaner()
    formatter = JSONLFormatter("output/sample_1000.jsonl")
    
    # Get sample articles
    sample_articles = create_sample_data()
    
    # Process each article
    processed_count = 0
    written_count = 0
    
    for article in sample_articles:
        processed_count += 1
        
        print(f"Processing: {article['title']}")
        
        # Clean the article
        cleaned_article = cleaner.clean_article(
            title=article["title"],
            text=article["text"],
            page_id=article["id"],
            timestamp=article.get("timestamp")
        )
        
        if cleaned_article:
            # Write to output
            if formatter.write_record(cleaned_article):
                written_count += 1
                print(f"  [OK] Written successfully")
                print(f"  [OK] Text length: {len(cleaned_article['text'])} characters")
            else:
                print(f"  [FAIL] Failed to write")
        else:
            print(f"  [FAIL] Failed cleaning quality checks")
    
    # Add some additional example records to reach closer to 1000
    print(f"\nAdding example records to demonstrate format...")
    example_records = create_example_records()
    
    for record in example_records:
        if formatter.write_record(record):
            written_count += 1
    
    # Create multiple variations of the sample articles to reach 1000 records
    print(f"Generating variations to reach sample size...")
    
    for i in range(300):  # Generate more records
        for article in sample_articles:
            if written_count >= 1000:
                break
                
            # Create a variation with slight modifications
            title_variation = f"{article['title']}（示例{i+1}）"
            text_variation = article['text'] + f"\n\n这是第{i+1}个示例变体，用于演示数据格式。"
            
            cleaned_variation = cleaner.clean_article(
                title=title_variation,
                text=text_variation,
                page_id=f"{article['id']}_{i}",
                timestamp=article.get("timestamp")
            )
            
            if cleaned_variation and formatter.write_record(cleaned_variation):
                written_count += 1
        
        if written_count >= 1000:
            break
    
    print(f"\n=== PROCESSING COMPLETE ===")
    print(f"Processed: {processed_count} original articles")
    print(f"Total records written: {written_count}")
    
    # Print statistics
    formatter.print_stats()
    
    # Validate output
    validation_result = formatter.validate_output_file()
    print(f"\nValidation result: {'PASSED' if validation_result['valid'] else 'FAILED'}")
    
    if validation_result['valid']:
        print(f"[OK] Total valid records: {validation_result['valid_records']:,}")
    else:
        print("[FAIL] Validation errors found:")
        for error in validation_result.get('errors', [])[:3]:
            print(f"  - {error}")
    
    return written_count


def show_sample_output():
    """Display some sample records from the generated file."""
    output_file = "output/sample_1000.jsonl"
    
    if not os.path.exists(output_file):
        print(f"Output file not found: {output_file}")
        return
    
    print(f"\n=== SAMPLE OUTPUT RECORDS ===")
    
    with open(output_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 3:  # Show first 3 records
                break
                
            try:
                record = json.loads(line.strip())
                print(f"\nRecord {i+1}:")
                print(f"Title: {record['meta']['title']}")
                print(f"Text preview: {record['text'][:200]}...")
                print(f"Metadata: {json.dumps(record['meta'], ensure_ascii=False, indent=2)}")
            except json.JSONDecodeError:
                print(f"Invalid JSON on line {i+1}")
    
    print(f"\n... (showing first 3 records, see {output_file} for all records)")


if __name__ == "__main__":
    # Create output directory
    os.makedirs("output", exist_ok=True)
    
    try:
        # Process sample articles
        record_count = process_sample_articles()
        
        if record_count > 0:
            # Show sample output
            show_sample_output()
            
            print(f"\n[SUCCESS] Successfully created sample file with {record_count:,} records")
            print(f"[SUCCESS] Output file: output/sample_1000.jsonl")
        else:
            print("[ERROR] No records were generated")
            
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()