import requests
import json
import time
import pandas as pd
import os
from urllib.parse import urlparse

# --- Configuration ---
# The base API endpoint we discovered.
API_BASE_URL = "https://api.tradeindia.com/seller-category/categories/seller-sub-category-entire-data"

# Headers that mimic a real browser request.
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.tradeindia.com/',
}

# --- Scraper Function ---
def scrape_all_pages(category_slug, session):
    """
    Scrapes all pages for a single category, handling duplicates and stopping
    when data becomes repetitive or the API stops responding.
    Returns the data and a flag indicating if an error occurred.
    """
    all_products = []
    seen_product_ids = set()
    current_page = 1
    stagnation_counter = 0
    api_outage_occurred = False

    print(f"--- Starting full scrape for category: {category_slug} ---")
    
    while True:
        params = {
            'url': category_slug,
            'page': current_page,
            'per_page': 50  # Set to the fixed optimal value of 50
        }
        
        try:
            response = session.get(API_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            json_response = response.json()
            product_list = json_response.get('listing', {}).get('data', [])

            if not product_list:
                print(f"No more data found at page {current_page}. Scrape for this category is complete.")
                break
            
            initial_unique_count = len(seen_product_ids)
            new_products_found = 0

            for item in product_list:
                product_id = item.get('product_id')
                if product_id and product_id not in seen_product_ids:
                    all_products.append(item)
                    seen_product_ids.add(product_id)
                    new_products_found += 1
            
            print(f"Successfully scraped page {current_page}. Found {len(product_list)} items, of which {new_products_found} were new.")

            if len(seen_product_ids) == initial_unique_count:
                stagnation_counter += 1
                print(f"Stagnation detected (count: {stagnation_counter}/3).")
            else:
                stagnation_counter = 0

            if stagnation_counter >= 3:
                print("Stopping scrape due to 3 consecutive pages with no new unique products.")
                break

            current_page += 1
            time.sleep(3)

        except (requests.exceptions.RequestException, ValueError) as e:
            # --- API OUTAGE LOGIC ---
            print(f"[ERROR] An API outage or JSON error occurred on page {current_page}: {e}")
            api_outage_occurred = True
            break
            
    return all_products, api_outage_occurred, current_page

# --- Main execution logic wrapped in a function ---
def run_scraper_logic(input_csv_file, output_dir):
    """
    Main function to be called as a module.
    """
    total_unique_products_scraped = 0
    total_api_outages = 0
    all_scraped_data = [] # This list will hold ALL scraped data from all sub-categories

    try:
        links_df = pd.read_csv(input_csv_file)
        os.makedirs(output_dir, exist_ok=True)

        total_sub_categories = len(links_df)
        print(f"\nFound {total_sub_categories} sub-categories to scrape.\n")
        
        # --- NEW LOGIC: RETRY OUTAGE LOOP ---
        links_to_process = list(links_df.to_dict('records'))
        
        with requests.Session() as session:
            session.headers.update(HEADERS)
            
            # Loop with an index to allow for retries of the same link
            index = 0
            while index < len(links_to_process):
                row = links_to_process[index]
                retries = 0
                
                while retries <= 1:
                    print("\n" + "*"*50)
                    print(f">>> Processing sub-category {index + 1} of {total_sub_categories} <<<")
                    
                    sub_category_url = row['links']
                    path_slug = urlparse(sub_category_url).path
                    
                    product_data, error_flag, failed_on_page = scrape_all_pages(path_slug, session)
                    
                    if not error_flag:
                        # Success: break out of the retry loop and move to the next link
                        total_unique_products_scraped += len(product_data)
                        sub_category_name = path_slug.strip('/').split('/')[-1]
                        output_filename = os.path.join(output_dir, f"{sub_category_name}.json")
                        
                        # Save the raw JSON as requested
                        with open(output_filename, 'w', encoding='utf-8') as f:
                            json.dump(product_data, f, ensure_ascii=False, indent=2)
                        print(f"\n[SUCCESS] Data for '{sub_category_name}' saved to {output_filename}")
                        
                        # Add the scraped data to the list to be returned
                        all_scraped_data.extend(product_data)
                        
                        break
                    
                    elif error_flag and retries == 0 and failed_on_page < 20:
                        # First failure: retry this link once if it's an early page
                        print("[!] First scrape attempt failed. Retrying this link once.")
                        time.sleep(120)  # Wait for 2 minutes as per the request
                        retries += 1
                        continue  # Re-run the inner retry loop for this link
                    
                    else:
                        # Second failure: log and skip this link
                        total_api_outages += 1
                        print("[FAILURE] Second attempt failed. Skipping this sub-category.")
                        break
                
                index += 1  # Move to the next link after all retries are exhausted

    except FileNotFoundError:
        print(f"[ERROR] The input file '{input_csv_file}' was not found.")
        return [], False
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
        return [], False
    finally:
        print("\n" + "="*50)
        print("--- SCRAPING RUN SUMMARY ---")
        print(f"Total Unique Products Scraped: {total_unique_products_scraped}")
        print(f"Total API Outages/Failures: {total_api_outages}")
        print("="*50)
    
    return all_scraped_data, True