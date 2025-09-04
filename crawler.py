import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

def crawl_category(url):
    """
    Crawls a webpage and extracts all unique sub-category links by targeting
    the specific class of the link's title.

    Args:
        url (str): The URL of the page to scrape.
    """
    print(f"[*] Fetching content from: {url}")
    
    # Standard headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    try:
        # Step 1: Fetch the HTML content of the page.
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        print("[+] Successfully fetched the webpage.")

        # Step 2: Parse the HTML with BeautifulSoup.
        soup = BeautifulSoup(response.text, 'html.parser')

        # Step 3: Find all `<a>` tags that contain 'title' in their class name.
        # This is the precise selector based on your screenshot.
        link_elements = soup.select('a[class*="title"]')
        
        # --- NEW FEATURE: FALLBACK LOGIC ---
        if not link_elements:
            print(f"[!] No links found with the primary class, trying fallback class...")
            link_elements = soup.select('a[class*="cat-main-heading"]')

        if not link_elements:
            print(f"[ERROR] No links were found with either class.")
            return []

        # Step 4: Loop through each link tag and extract its URL.
        discovered_links = []
        for link_tag in link_elements:
            relative_url = link_tag.get('href')
            if relative_url:
                absolute_url = urljoin(url, relative_url)
                discovered_links.append(absolute_url)

        unique_links = list(set(discovered_links))
        print(f"[+] Found {len(unique_links)} unique links.")
        return unique_links

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch the URL {url}. Error: {e}")
        return []

# --- Main execution logic wrapped in a function ---
def run_crawler_logic(target_url, output_filename):
    """
    Main function to be called as a module.
    """
    links = crawl_category(target_url)
    
    if links:
        try:
            # Create a pandas DataFrame with the specified column name.
            df = pd.DataFrame(sorted(links), columns=['links'])
            
            # Save the DataFrame to a CSV file.
            df.to_csv(output_filename, index=False)
            
            print(f"\n[SUCCESS] Targeted links saved to: {output_filename}")
            return True

        except IOError as e:
            print(f"[ERROR] Could not write to file {output_filename}. Error: {e}")
            return False
    else:
        print("[FAILURE] No links were discovered.")
        return False