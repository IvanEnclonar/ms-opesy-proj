import csv
import argparse
import sys
from urllib.parse import urlparse

def verify_csv(file_path, valid_domain):
    print(f"🔎 Inspecting file: {file_path}")
    print(f"🎯 Target Domain scope: {valid_domain}\n")

    urls_seen = set()
    duplicates = []
    invalid_domain_urls = []
    total_rows = 0

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Check if 'url' column exists
            if 'url' not in reader.fieldnames:
                print("❌ Error: CSV must have a 'url' column.")
                return

            for row in reader:
                total_rows += 1
                url = row['url']

                # --- Check 1: Duplicates ---
                if url in urls_seen:
                    duplicates.append(url)
                else:
                    urls_seen.add(url)

                # --- Check 2: Domain Validation ---
                parsed_url = urlparse(url)
                netloc = parsed_url.netloc.lower()
                
                # We remove 'www.' to ensure dlsu.edu.ph matches www.dlsu.edu.ph
                clean_target = valid_domain.replace("www.", "")
                
                if not netloc.endswith(clean_target):
                    invalid_domain_urls.append(url)

    except FileNotFoundError:
        print(f"❌ Error: File '{file_path}' not found.")
        return
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return

    # --- Report Results ---
    print("-" * 40)
    print(f"📊 Summary for {file_path}")
    print("-" * 40)
    print(f"Total Rows Processed:      {total_rows}")
    print(f"Unique URLs:               {len(urls_seen)}")
    
    # 1. Duplicates Report
    if duplicates:
        print(f"⚠️  Duplicate URLs found:    {len(duplicates)}")
        print("   (Showing first 5):")
        for d in duplicates[:5]:
            print(f"   - {d}")
    else:
        print(f"✅ No duplicates found.")

    # 2. Domain Report
    if invalid_domain_urls:
        print(f"⚠️  External/Invalid Domains: {len(invalid_domain_urls)}")
        print("   (These do not match the target domain):")
        for u in invalid_domain_urls[:5]:
            print(f"   - {u}")
    else:
        print(f"✅ All URLs belong to {valid_domain}.")
    
    print("-" * 40)

    if not duplicates and not invalid_domain_urls:
        print("🎉 The data is CLEAN!")
    else:
        print("🧹 The data needs cleaning.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify scraped CSV data.")
    parser.add_argument("file", help="Path to the CSV file (e.g., output/distributed_checkpoint_60s.csv)")
    parser.add_argument("--domain", default="dlsu.edu.ph", help="The domain to validate against (default: dlsu.edu.ph)")
    
    args = parser.parse_args()
    
    verify_csv(args.file, args.domain)