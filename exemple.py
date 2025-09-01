import requests
from bs4 import BeautifulSoup
 
def fetch_articles(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
 
    try :
        response = requests.get(url, headers=headers)
        response.raise_for_status()
 
        soup = BeautifulSoup(response.text, 'html.parser')
 
        main_tag = soup.find('main')
        if not main_tag:
            print("no <main> tag found")
   
        articles = main_tag.find_all('article')
        for article in articles :
            img_div = article.find(
                'div',
                class_='post-thumbnail'
            )
            img_tag = img_div.find('img') if img_div else None
            img_url = extract_img_url(img_tag)
            print(img_url)
 
        return []
    except requests.exceptions.RequestException as e :
        print(f"Error: {e}")
        return []
 
def extract_img_url(img_tag):
    if not img_tag:
        return None
    url_attrs = [
        'src',
        'data-lazy-srcset',
        'data-lazy-src'
    ]
    for attr in url_attrs:
        if img_tag.has_attr(attr):
            url = img_tag[attr]
            if url and url.startswith('https://'):
                return url
    return None
 
url = "https://www.blogdumoderateur.com/web/"
articles = fetch_articles(url)
 