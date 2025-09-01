import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
from dateutil import parser as dateutil_parser
from pymongo import MongoClient, ASCENDING
import time

# Configuration de la base de données
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bdm_db"
COLLECTION = "articles"

# Connexion à MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
articles_col = db[COLLECTION]
articles_col.create_index([("url", ASCENDING)], unique=True, sparse=True)

#-------------------------------------------------------------------------------------------------------------------

def nettoyer_texte(texte):
    """Nettoie et normalise le texte"""
    if not texte:
        return ""
    texte = re.sub(r'\s+', ' ', texte.strip())
    texte = re.sub(r'[\u00A0\u2000-\u200B\u2028\u2029]', ' ', texte)
    return texte.strip()

#-------------------------------------------------------------------------------------------------------------------

def extraire_url_image(img_tag, base_url):
    """Récupère l'URL d'une image depuis ses attributs"""
    if not img_tag:
        return None
    
    attributs = ["data-src", "data-lazy-src", "data-original", "src", "data-srcset"]
    
    for attr in attributs:
        url = img_tag.get(attr)
        if url:
            if "," in url:  # cas des srcset avec plusieurs tailles
                url = url.split(",")[0].split()[0]
            return urljoin(base_url, url.strip())
    return None

#-------------------------------------------------------------------------------------------------------------------

def convertir_date_francaise(texte_date):
    """Convertit une date française en format AAAAMMJJ"""
    if not texte_date:
        return None
    
    mois_francais = {
        'janvier': '01', 'février': '02', 'fevrier': '02', 'mars': '03', 
        'avril': '04', 'mai': '05', 'juin': '06', 'juillet': '07', 
        'août': '08', 'aout': '08', 'septembre': '09', 'octobre': '10', 
        'novembre': '11', 'décembre': '12', 'decembre': '12'
    }
    
    texte_date = texte_date.strip()
    
    # Cherche le format "jour mois année"
    match = re.search(r'(\d{1,2})\s+([^\s,]+)\s+(\d{4})', texte_date, flags=re.IGNORECASE)
    if match:
        jour = int(match.group(1))
        nom_mois = match.group(2).lower()
        annee = int(match.group(3))
        numero_mois = mois_francais.get(nom_mois)
        if numero_mois:
            return f"{annee:04d}{numero_mois}{jour:02d}"
    
    # Cherche le format ISO
    iso_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', texte_date)
    if iso_match:
        return f"{iso_match.group(1)}{iso_match.group(2)}{iso_match.group(3)}"
    
    # Dernière tentative avec le parseur automatique
    try:
        dt = dateutil_parser.parse(texte_date, dayfirst=True, fuzzy=True)
        return dt.strftime("%Y%m%d")
    except:
        return None

#-------------------------------------------------------------------------------------------------------------------

def scraper_article_bdm(url, session=None, verbose=False):
    """
    Scrape un article du Blog du Modérateur
    Retourne toutes les infos demandées dans le TP
    """
    if session is None:
        session = requests.Session()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        if verbose:
            print(f"Impossible de charger {url}: {e}")
        return None
    
    base_url = response.url
    soup = BeautifulSoup(response.text, "html.parser")

    # Supprimer les scripts et styles pour avoir un contenu propre
    for script in soup(["script", "style"]):
        script.decompose()

    # Trouver la zone principale de l'article
    article = soup.find("article")
    if not article:
        article = soup.find("main")
    if not article:
        article = soup.find("body")

    # 1. TITRE
    titre = None
    h1 = soup.find('h1')
    if h1:
        titre = nettoyer_texte(h1.get_text())
    
    if not titre:
        meta_title = soup.find("meta", property="og:title")
        if meta_title:
            titre = nettoyer_texte(meta_title.get("content"))

    # 2. IMAGE MINIATURE
    thumbnail = None
    meta_og = soup.find("meta", property="og:image")
    if meta_og and meta_og.get("content"):
        thumbnail = urljoin(base_url, meta_og["content"])
    
    if not thumbnail and article:
        img_principale = article.find("img")
        if img_principale:
            thumbnail = extraire_url_image(img_principale, base_url)

    # 3. SOMMAIRE
    sommaire = []
    # Chercher un titre "Sommaire" suivi d'une liste
    titres_sommaire = soup.find_all(lambda tag: tag.name in ['h2', 'h3', 'div'] 
                                   and 'sommaire' in tag.get_text().lower())
    
    for titre_som in titres_sommaire:
        liste = titre_som.find_next(['ol', 'ul'])
        if liste:
            for item in liste.find_all('li'):
                texte = nettoyer_texte(item.get_text())
                if texte and len(texte) > 2:
                    sommaire.append(texte)
            break

    # 4. SOUS-CATÉGORIE
    sous_categorie = None
    # Chercher dans les breadcrumbs (fil d'Ariane)
    breadcrumb = soup.find(['nav', 'div'], class_=re.compile(r'breadcrumb', re.I))
    if breadcrumb:
        liens = breadcrumb.find_all('a')
        if len(liens) >= 2:
            sous_categorie = nettoyer_texte(liens[-2].get_text())
    
    # Alternative: meta article:section
    if not sous_categorie:
        meta_section = soup.find("meta", {"name": "article:section"})
        if meta_section:
            sous_categorie = nettoyer_texte(meta_section.get("content"))

    # 5. RÉSUMÉ/CHAPÔ
    resume = None
    # Chercher les éléments avec des classes typiques de résumé
    resume_elem = soup.find(['p', 'div'], class_=re.compile(r'(chapo|lead|intro|excerpt)', re.I))
    if resume_elem:
        resume = nettoyer_texte(resume_elem.get_text())
    
    # Alternative: meta description
    if not resume:
        meta_desc = soup.find("meta", {"name": "description"})
        if meta_desc:
            resume = nettoyer_texte(meta_desc.get("content"))

    # 6. DATE DE PUBLICATION
    date_aaaammjj = None
    texte_date = None
    
    # Chercher l'élément time
    time_tag = soup.find('time')
    if time_tag:
        texte_date = time_tag.get('datetime') or nettoyer_texte(time_tag.get_text())
    
    # Chercher "Publié le..."
    if not texte_date:
        pub_match = re.search(r'Publié\s+le?\s+([^|]+)', soup.get_text(), re.I)
        if pub_match:
            texte_date = pub_match.group(1)
    
    if texte_date:
        date_aaaammjj = convertir_date_francaise(texte_date)

    # 7. AUTEUR
    auteur = None
    # Chercher les liens avec rel="author"
    lien_auteur = soup.find('a', rel='author')
    if lien_auteur:
        auteur = nettoyer_texte(lien_auteur.get_text())
    
    # Alternative: chercher les classes "author"
    if not auteur:
        elem_auteur = soup.find(['span', 'div'], class_=re.compile(r'author', re.I))
        if elem_auteur:
            auteur = nettoyer_texte(elem_auteur.get_text())

    # 8. CONTENU DE L'ARTICLE
    contenu_texte = ""
    if article:
        # Trouver la zone de contenu principal
        zone_contenu = article.find(['div'], class_=re.compile(r'(content|post-content)', re.I))
        if not zone_contenu:
            zone_contenu = article
        
        # Extraire les paragraphes et titres
        paragraphes = []
        for elem in zone_contenu.find_all(['p', 'h2', 'h3', 'h4', 'h5', 'h6']):
            # Ignorer les éléments de navigation ou publicité
            if elem.find_parent(['nav', 'aside']):
                continue
            
            texte = nettoyer_texte(elem.get_text())
            if texte and len(texte) > 10:
                if elem.name.startswith('h'):
                    paragraphes.append(f"\n{texte}\n")  # Espacer les titres
                else:
                    paragraphes.append(texte)
        
        contenu_texte = "\n\n".join(paragraphes).strip()
        contenu_texte = re.sub(r'\n{3,}', '\n\n', contenu_texte)  # Nettoyer les sauts de ligne

    # 9. IMAGES DE L'ARTICLE
    images = []
    if article:
        zone_contenu = article.find(['div'], class_=re.compile(r'content', re.I)) or article
        
        for img in zone_contenu.find_all('img'):
            url_img = extraire_url_image(img, base_url)
            if not url_img:
                continue
            
            # Ignorer les petites images (probablement des icônes)
            largeur = img.get('width')
            hauteur = img.get('height')
            if largeur and hauteur:
                try:
                    if int(largeur) < 100 or int(hauteur) < 100:
                        continue
                except:
                    pass
            
            # Récupérer la légende
            legende = None
            # Chercher dans figcaption
            parent_figure = img.find_parent('figure')
            if parent_figure:
                figcaption = parent_figure.find('figcaption')
                if figcaption:
                    legende = nettoyer_texte(figcaption.get_text())
            
            # Alternative: attributs alt ou title
            if not legende:
                legende = nettoyer_texte(img.get('alt', '')) or nettoyer_texte(img.get('title', ''))
            
            # Éviter les doublons avec le thumbnail
            if url_img != thumbnail:
                images.append({
                    "url": url_img,
                    "caption": legende or ""
                })

    # Construire le résultat final
    resultat = {
        "url": base_url,
        "title": titre or "",
        "thumbnail": thumbnail or "",
        "sommaire": sommaire,
        "subcategory": sous_categorie or "",
        "summary": resume or "",
        "date": date_aaaammjj or "",
        "author": auteur or "",
        "content": contenu_texte,
        "images": images,
        "scraped_at": datetime.utcnow().isoformat()
    }

    if verbose:
        print(f"Scrapé: {resultat['title'][:50]}...")
        print(f"{resultat['date']} | 👤 {resultat['author']} | {len(resultat['images'])} images")
    
    return resultat

#-------------------------------------------------------------------------------------------------------------------

def sauvegarder_en_base(article):
    """Sauvegarde un article dans MongoDB"""
    if not article or not article.get("url"):
        raise ValueError("L'article doit avoir une URL pour être sauvegardé")
    
    try:
        resultat = articles_col.update_one(
            {"url": article["url"]},
            {"$set": article},
            upsert=True
        )
        return resultat
    except Exception as e:
        print(f"Erreur lors de la sauvegarde: {e}")
        raise

#-------------------------------------------------------------------------------------------------------------------

def chercher_articles_par_categorie(categorie=None, sous_categorie=None, limite=100):
    """Trouve les articles d'une catégorie ou sous-catégorie"""
    requete = {}
    
    if categorie:
        requete["subcategory"] = {"$regex": f"^{re.escape(categorie)}$", "$options": "i"}
    
    if sous_categorie:
        requete["subcategory"] = {"$regex": f"^{re.escape(sous_categorie)}$", "$options": "i"}

    try:
        cursor = articles_col.find(requete).limit(limite).sort("scraped_at", -1)
        return list(cursor)
    except Exception as e:
        print(f"Erreur lors de la recherche: {e}")
        return []

#-------------------------------------------------------------------------------------------------------------------

def recherche_avancee(titre_contient=None, auteur=None, date_debut=None, date_fin=None, 
                     categorie=None, sous_categorie=None, limite=100):
    """Recherche avancée dans les articles"""
    requete = {}
    
    if titre_contient:
        requete["title"] = {"$regex": re.escape(titre_contient), "$options": "i"}
    
    if auteur:
        requete["author"] = {"$regex": re.escape(auteur), "$options": "i"}
    
    if date_debut or date_fin:
        date_requete = {}
        if date_debut:
            date_requete["$gte"] = date_debut
        if date_fin:
            date_requete["$lte"] = date_fin
        requete["date"] = date_requete
    
    if sous_categorie:
        requete["subcategory"] = {"$regex": re.escape(sous_categorie), "$options": "i"}

    try:
        cursor = articles_col.find(requete).limit(limite).sort("date", -1)
        return list(cursor)
    except Exception as e:
        print(f"Erreur dans la recherche: {e}")
        return []

# Test du script
if __name__ == "__main__":
    print("Test du scraper avec le Blog du Modérateur")
    
    url_test = "https://www.blogdumoderateur.com/100-outils-ia-plus-utilises-monde-ete-2025/"
    
    print(f"Scraping: {url_test}")
    donnees = scraper_article_bdm(url_test, verbose=True)
    
    if donnees:
        print(f"\n📋 Résultats:")
        print(f"Titre: {donnees['title']}")
        print(f"Date: {donnees['date']}")
        print(f"Auteur: {donnees['author']}")
        print(f"Catégorie: {donnees['subcategory']}")
        print(f"Résumé: {donnees['summary'][:100]}...")
        print(f"Contenu: {len(donnees['content'])} caractères")
        print(f"Images: {len(donnees['images'])}")
        print(f"Sommaire: {len(donnees['sommaire'])} éléments")
        
        # Sauvegarder
        try:
            resultat_sauvegarde = sauvegarder_en_base(donnees)
            print(f"Sauvegardé: {resultat_sauvegarde.acknowledged}")
        except Exception as e:
            print(f"Erreur sauvegarde: {e}")
        
        # Tester la recherche
        print(f"\n🔍 Test recherche par catégorie '{donnees['subcategory']}':")
        articles_trouves = chercher_articles_par_categorie(sous_categorie=donnees['subcategory'])
        print(f"Trouvé {len(articles_trouves)} articles")
        
        for article in articles_trouves[:3]:
            print(f"- {article.get('title', 'Sans titre')} ({article.get('date', 'Sans date')})")
    else:
        print("echec")