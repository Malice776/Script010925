import requests
import re
from bs4 import BeautifulSoup
from bs4.element import NavigableString
from urllib.parse import urljoin
from datetime import datetime
from dateutil import parser as dateutil_parser
from pymongo import MongoClient, ASCENDING
import time

# --- Configuration MongoDB ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bdm_db"
COLLECTION = "articles"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
articles_col = db[COLLECTION]
# create an index to avoid duplicates (by url)
articles_col.create_index([("url", ASCENDING)], unique=True, sparse=True)


# --- Helpers ---

def _first_nonempty_text(el):
    """Extract first non-empty text from element."""
    if not el:
        return None
    txt = el.get_text(strip=True)
    return txt if txt else None

def _get_img_url(img_tag, base_url):
    """Get image URL from various possible attributes."""
    if not img_tag:
        return None
    
    for attr in ["data-src", "data-lazy-src", "data-original", "src", "data-srcset"]:
        url = img_tag.get(attr)
        if url:
            # if srcset, pick first url
            if attr == "data-srcset" or "," in url:
                url = url.split(",")[0].split()[0]
            return urljoin(base_url, url.strip())
    return None

# French month names -> number (lowercase)
FRENCH_MONTHS = {
    'janvier': '01', 'février': '02', 'fevrier': '02', 'mars': '03', 'avril': '04', 'mai': '05', 'juin': '06',
    'juillet': '07', 'août': '08', 'aout': '08', 'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12', 'decembre': '12'
}

def parse_french_date(text):
    """
    Try to parse French dates and return AAAAMMJJ format.
    Handles formats like: 'Publié le 28 août 2025 à 11h05' -> '20250828'
    """
    if not text:
        return None
    
    # Clean the text
    text = text.strip()
    
    # Try to find pattern day month year
    m = re.search(r'(\d{1,2})\s+([^\s,]+)\s+(\d{4})', text, flags=re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = FRENCH_MONTHS.get(month_name)
        if month:
            return f"{year:04d}{month}{day:02d}"
    
    # Try to find ISO date format
    iso_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if iso_match:
        return f"{iso_match.group(1)}{iso_match.group(2)}{iso_match.group(3)}"
    
    # Fallback: try dateutil parser
    try:
        dt = dateutil_parser.parse(text, dayfirst=True, fuzzy=True)
        return dt.strftime("%Y%m%d")
    except Exception:
        return None


def clean_text(text):
    """Clean and normalize text content."""
    if not text:
        return ""
    
    # Remove extra whitespace and normalize
    text = re.sub(r'\s+', ' ', text.strip())
    # Remove special characters that might cause issues
    text = re.sub(r'[\u00A0\u2000-\u200B\u2028\u2029]', ' ', text)
    return text.strip()


# --- Main scraping function ---

def scrape_article(url, session=None, verbose=False):
    """
    Scrape a BDM article and return a dict with required fields.
    Returns: title, thumbnail, sommaire, subcategory, summary, date (AAAAMMJJ),
    author, content, images, url
    """
    if session is None:
        session = requests.Session()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        r = session.get(url, headers=headers, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        if verbose:
            print(f"Error fetching {url}: {e}")
        return None
    
    base_url = r.url
    soup = BeautifulSoup(r.text, "html.parser")

    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Locate main article container
    article = soup.find("article")
    if not article:
        article = soup.find("div", {"class": re.compile(r'(entry-content|post|article|article-wrapper|single-post)', re.I)})
    if not article:
        article = soup.find("main")
    if not article:
        article = soup.find("body")

    # --- Title ---
    title = None
    # Try h1 first (most likely article title)
    title_tag = soup.find('h1')
    if title_tag:
        title = clean_text(title_tag.get_text())
    
    # Fallback: try meta title or og:title
    if not title:
        meta_title = soup.find("meta", property="og:title") or soup.find("title")
        if meta_title:
            content = meta_title.get("content") or meta_title.get_text()
            if content:
                title = clean_text(content)

    # --- Thumbnail ---
    thumbnail = None
    # Try meta og:image first
    meta_og = soup.find("meta", property="og:image")
    if meta_og and meta_og.get("content"):
        thumbnail = urljoin(base_url, meta_og["content"])
    
    # Try featured image or first figure
    if not thumbnail:
        # Look for featured image classes
        featured_img = soup.find("img", class_=re.compile(r'(featured|hero|main|thumbnail)', re.I))
        if featured_img:
            thumbnail = _get_img_url(featured_img, base_url)
    
    # Try first figure/img in article
    if not thumbnail and article:
        fig = article.find("figure")
        if fig:
            img = fig.find("img")
            if img:
                thumbnail = _get_img_url(img, base_url)
        
        # Fallback: first image in article
        if not thumbnail:
            first_img = article.find("img")
            if first_img:
                thumbnail = _get_img_url(first_img, base_url)

    # --- Sommaire (Table of Contents) ---
    sommaire = []
    # Look for explicit sommaire section
    toc_headers = soup.find_all(lambda tag: tag.name in ['h2', 'h3', 'h4', 'div', 'p', 'span'] 
                               and re.search(r'sommaire|table.?des.?matières|plan.?de.?l.?article', tag.get_text(), re.I))
    
    if toc_headers:
        for toc_header in toc_headers:
            # Find next list after sommaire header
            next_list = toc_header.find_next(['ol', 'ul'])
            if next_list:
                for li in next_list.find_all('li'):
                    txt = clean_text(li.get_text())
                    if txt and len(txt) > 2:  # avoid single characters
                        sommaire.append(txt)
                break  # Take first valid sommaire found
    
    # Alternative: look for navigation with table-of-contents class
    if not sommaire:
        toc_nav = soup.find(['nav', 'div'], class_=re.compile(r'(toc|table-of-contents|sommaire)', re.I))
        if toc_nav:
            for item in toc_nav.find_all(['li', 'a']):
                txt = clean_text(item.get_text())
                if txt and len(txt) > 2:
                    sommaire.append(txt)

    # --- Subcategory ---
    subcategory = None
    # Try breadcrumbs first
    breadcrumb = soup.find(['nav', 'div', 'ol', 'ul'], class_=re.compile(r'(breadcrumb|breadcrumbs|fil.?ariane)', re.I))
    if breadcrumb:
        links = breadcrumb.find_all('a')
        # Get the last category before the article title
        if len(links) >= 2:
            subcategory = clean_text(links[-2].get_text())
        elif links:
            subcategory = clean_text(links[-1].get_text())
    
    # Try meta article:section
    if not subcategory:
        meta_section = soup.find("meta", {"name": "article:section"}) or soup.find("meta", {"property": "article:section"})
        if meta_section and meta_section.get("content"):
            subcategory = clean_text(meta_section["content"])
    
    # Try category links near header
    if not subcategory:
        cat_links = soup.find_all("a", href=re.compile(r'/(categor|tag|theme)/', re.I))
        if cat_links:
            subcategory = clean_text(cat_links[0].get_text())

    # --- Summary/Chapô ---
    summary = None
    # Look for elements with intro/lead/chapo classes
    summary_candidates = soup.find_all(['p', 'div'], class_=re.compile(r'(chapo|lead|intro|excerpt|summary|extrait)', re.I))
    if summary_candidates:
        summary = clean_text(summary_candidates[0].get_text())
    
    # Alternative: meta description
    if not summary:
        meta_desc = soup.find("meta", {"name": "description"}) or soup.find("meta", property="og:description")
        if meta_desc and meta_desc.get("content"):
            summary = clean_text(meta_desc["content"])
    
    # Fallback: first paragraph after title (if reasonable length)
    if not summary and title_tag:
        next_p = title_tag.find_next('p')
        if next_p:
            p_text = clean_text(next_p.get_text())
            if 50 <= len(p_text) <= 300:  # reasonable summary length
                summary = p_text

    # --- Date ---
    date_aaaammjj = None
    date_text = None
    
    # Look for time element
    time_tag = soup.find('time')
    if time_tag:
        date_text = time_tag.get('datetime') or clean_text(time_tag.get_text())
    
    # Look for publication date text patterns
    if not date_text:
        pub_patterns = [
            r'Publié\s+le?\s+([^|]+)',
            r'Published\s+on\s+([^|]+)',
            r'(\d{1,2}\s+\w+\s+\d{4})',
            r'(\d{4}-\d{2}-\d{2})'
        ]
        
        for pattern in pub_patterns:
            pub_match = re.search(pattern, soup.get_text(), re.I)
            if pub_match:
                date_text = pub_match.group(1)
                break
    
    # Parse date to AAAAMMJJ format
    if date_text:
        date_aaaammjj = parse_french_date(date_text)

    # --- Author ---
    author = None
    # Look for rel="author" links
    author_link = soup.find('a', rel='author')
    if author_link:
        author = clean_text(author_link.get_text())
    
    # Look for author classes
    if not author:
        author_elem = soup.find(['span', 'div', 'p'], class_=re.compile(r'(author|auteur|byline|writer)', re.I))
        if author_elem:
            author = clean_text(author_elem.get_text())
    
    # Look for author in byline/meta information
    if not author:
        byline = soup.find(lambda tag: tag and re.search(r'(par|by)\s+([^|,\n]+)', tag.get_text(), re.I))
        if byline:
            author_match = re.search(r'(par|by)\s+([^|,\n]+)', byline.get_text(), re.I)
            if author_match:
                author = clean_text(author_match.group(2))

    # --- Content ---
    content_text = ""
    if article:
        # Find main content area
        content_area = article.find(['div', 'section'], class_=re.compile(r'(entry-content|post-content|article-content|content|post-body)', re.I))
        if not content_area:
            content_area = article
        
        # Extract paragraphs and headings
        paragraphs = []
        for elem in content_area.find_all(['p', 'h2', 'h3', 'h4', 'h5', 'h6']):
            # Skip elements that are part of navigation, ads, or metadata
            if elem.find_parent(['nav', 'aside']) or elem.find_parent(class_=re.compile(r'(nav|sidebar|ad|meta|social)', re.I)):
                continue
            
            txt = clean_text(elem.get_text())
            if txt and len(txt) > 10:  # avoid very short fragments
                if elem.name.startswith('h'):
                    paragraphs.append(f"\n{txt}\n")  # Add spacing around headings
                else:
                    paragraphs.append(txt)
        
        content_text = "\n\n".join(paragraphs).strip()
        # Clean up multiple newlines
        content_text = re.sub(r'\n{3,}', '\n\n', content_text)

    # --- Images ---
    images = []
    if article:
        # Find images in content area
        content_area = article.find(['div', 'section'], class_=re.compile(r'(entry-content|post-content|article-content|content)', re.I))
        if not content_area:
            content_area = article
        
        for img in content_area.find_all('img'):
            img_url = _get_img_url(img, base_url)
            if not img_url:
                continue
            
            # Skip very small images (likely icons or decorations)
            width = img.get('width')
            height = img.get('height')
            if width and height:
                try:
                    if int(width) < 100 or int(height) < 100:
                        continue
                except (ValueError, TypeError):
                    pass
            
            # Get caption
            caption = None
            # Check parent figure for figcaption
            parent_fig = img.find_parent('figure')
            if parent_fig:
                figcaption = parent_fig.find('figcaption')
                if figcaption:
                    caption = clean_text(figcaption.get_text())
            
            # Fallback to alt or title attributes
            if not caption:
                caption = clean_text(img.get('alt', '')) or clean_text(img.get('title', ''))
            
            # Skip if this image is the same as thumbnail
            if img_url != thumbnail:
                images.append({
                    "url": img_url,
                    "caption": caption or ""
                })

    # --- Compose result ---
    result = {
        "url": base_url,
        "title": title or "",
        "thumbnail": thumbnail or "",
        "sommaire": sommaire,
        "subcategory": subcategory or "",
        "summary": summary or "",
        "date": date_aaaammjj or "",
        "author": author or "",
        "content": content_text,
        "images": images,
        "scraped_at": datetime.utcnow().isoformat()
    }

    if verbose:
        print(f"Scraped: {result['title'][:50]}...")
        print(f"Date: {result['date']}, Author: {result['author']}")
        print(f"Images: {len(result['images'])}, Sommaire items: {len(result['sommaire'])}")
    
    return result


# --- Save to MongoDB ---
def save_article_to_mongo(article_dict):
    """
    Insert or update article into MongoDB collection 'articles'.
    Uses article['url'] as unique key.
    """
    if not article_dict or "url" not in article_dict or not article_dict["url"]:
        raise ValueError("Article must contain valid 'url' to be saved")
    
    try:
        # Upsert by url
        result = articles_col.update_one(
            {"url": article_dict["url"]},
            {"$set": article_dict},
            upsert=True
        )
        return result
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")
        raise


# --- Query functions ---
def get_articles_by_category(category=None, subcategory=None, limit=100):
    """
    Return articles matching category or subcategory.
    If both None -> return all (capped by limit).
    """
    query = {}
    
    if category:
        query["$or"] = [
            {"subcategory": {"$regex": f"^{re.escape(category)}$", "$options": "i"}},
            {"category": {"$regex": f"^{re.escape(category)}$", "$options": "i"}}
        ]
    
    if subcategory:
        subcat_condition = {"subcategory": {"$regex": f"^{re.escape(subcategory)}$", "$options": "i"}}
        if "$or" in query:
            query = {"$and": [query, subcat_condition]}
        else:
            query = subcat_condition

    try:
        cursor = articles_col.find(query).limit(limit).sort("scraped_at", -1)
        return list(cursor)
    except Exception as e:
        print(f"Error querying MongoDB: {e}")
        return []


def search_articles(title_substring=None, author=None, date_start=None, date_end=None, 
                   category=None, subcategory=None, limit=100):
    """
    Advanced search function for articles.
    date_start/date_end should be in AAAAMMJJ format.
    """
    query = {}
    
    if title_substring:
        query["title"] = {"$regex": re.escape(title_substring), "$options": "i"}
    
    if author:
        query["author"] = {"$regex": re.escape(author), "$options": "i"}
    
    if date_start or date_end:
        date_query = {}
        if date_start:
            date_query["$gte"] = date_start
        if date_end:
            date_query["$lte"] = date_end
        query["date"] = date_query
    
    if category:
        query["$or"] = query.get("$or", []) + [
            {"subcategory": {"$regex": re.escape(category), "$options": "i"}}
        ]
    
    if subcategory:
        subcat_condition = {"subcategory": {"$regex": re.escape(subcategory), "$options": "i"}}
        if "$or" in query:
            query = {"$and": [query, subcat_condition]}
        else:
            query.update(subcat_condition)

    try:
        cursor = articles_col.find(query).limit(limit).sort("date", -1)
        return list(cursor)
    except Exception as e:
        print(f"Error in search: {e}")
        return []


# exemple de comment l'utiliser
if __name__ == "__main__":
    # Test with a BDM article
    TEST_URL = "https://www.blogdumoderateur.com/100-outils-ia-plus-utilises-monde-ete-2025/"
    
    print(f"Scraping: {TEST_URL}")
    data = scrape_article(TEST_URL, verbose=True)
    
    if data:
        print(f"\nTitle: {data['title']}")
        print(f"Date: {data['date']}")
        print(f"Author: {data['author']}")
        print(f"Subcategory: {data['subcategory']}")
        print(f"Summary: {data['summary'][:100]}...")
        print(f"Content length: {len(data['content'])} chars")
        print(f"Images: {len(data['images'])}")
        print(f"Sommaire items: {len(data['sommaire'])}")
        
        # je met dans mogodb 
        try:
            save_result = save_article_to_mongo(data)
            print(f"Saved to MongoDB: {save_result.acknowledged}")
        except Exception as e:
            print(f"Error saving: {e}")
        
        # test
        print(f"\nQuerying articles by subcategory '{data['subcategory']}':")
        found_articles = get_articles_by_category(subcategory=data['subcategory'])
        print(f"Found {len(found_articles)} articles")
        for article in found_articles[:3]:
            print(f"- {article.get('title', 'No title')} ({article.get('date', 'No date')})")
    else:
        print("Failed to scrape article")