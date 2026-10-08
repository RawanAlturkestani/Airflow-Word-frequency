from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import urllib.request
import time
import glob, os
import json
import re

from pull import catalog

# Common filler words that would otherwise dominate the chart
STOP_WORDS = set("""
a about above after again against all also am an and any are as at be because been
before being below between both but by can could did do does doing down during each
few for from further had has have having he her here hers him his how i if in into
is it its itself just me more most my no nor not now of off on once only or other
our out over own same she should so some such than that the their them then there
these they this those through to too under until up us very was we were what when
where which while who whom why will with would you your via per vs etc
""".split())

DATA_DIR = '/opt/airflow/data'   # same folder as OUTPUT_DIR in pull.py


def p(name):
    return os.path.join(DATA_DIR, name)


def combine():
    with open(p('combo.txt'), 'w') as outfile:
        for file in glob.glob(p("*.html")):
            with open(file) as infile:
                outfile.write(infile.read())

def titles():
    from bs4 import BeautifulSoup
    def store_json(data,file):
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            print('wrote file: ' + file)

    # Combined html of every page downloaded by task_one
    with open(p('combo.txt'), encoding='utf-8') as f:
        html = f.read()

    # Each saved page has its own <html> tag, so cut the combined text at each one
    # (written without a zero-width re.split, which does not work on Python 3.6)
    starts = [m.start() for m in re.finditer(r'(?i)<html[\s>]', html)]
    if not starts:
        pages = [html]
    else:
        pages = [html[a:b] for a, b in zip(starts, starts[1:] + [len(html)])]
    texts = []

    for page in pages:
        if not page.strip():
            continue
        soup = BeautifulSoup(page, "html.parser")

        # remove parts that are not article text
        for tag in soup(['script', 'style', 'noscript', 'nav', 'header', 'footer', 'aside', 'form', 'svg']):
            tag.decompose()

        # use the main article if the page marks one, otherwise the whole body
        root = soup.find('article') or soup.find('main') or soup.body or soup

        # The part that includes headings, paragraphs and list items, in page order 
        found = 0
        for item in root.find_all(['h1', 'h2', 'h3', 'h4', 'p', 'li']):
            # skip a list item that only wraps another list or paragraph, to avoid counting text twice
            if item.name == 'li' and item.find(['p', 'ul', 'ol']):
                continue
            text = ' '.join(item.get_text(' ', strip=True).split())
            if text:
                texts.append(text)
                found += 1

        # pages like the MIT catalog keep working: nothing found means fall back to h3 only
        if found == 0:
            texts.extend(h.text for h in soup.find_all('h3'))

    store_json(texts, p('titles.json'))


def clean():
   def store_json(data,file):
       with open(file, 'w', encoding='utf-8') as f:
           json.dump(data, f, ensure_ascii=False, indent=4)
           print('wrote file: ' + file)

   with open(p('titles.json')) as file:
       titles = json.load(file)

   cleaned = []
   for title in titles:
       # punctuation, digits and symbols become spaces, so "College-Level" -> "college level"
       # (letters with accents are kept)
       text = re.sub(r"[\W\d_]+", " ", title.lower())
       # drop one character words and stop words such as "and", "in", "of"
       words = [w for w in text.split() if len(w) > 1 and w not in STOP_WORDS]
       cleaned.append(' '.join(words))

   store_json(cleaned, p('titles_clean.json'))


def count_words():
     from collections import Counter
     def store_json(data,file):
       with open(file, 'w', encoding='utf-8') as f:
           json.dump(data, f, ensure_ascii=False, indent=4)
           print('wrote file: ' + file)
           
     with open(p('titles_clean.json')) as file:
            titles = json.load(file)
            words = []

            # extract words and flatten
            for title in titles:
                words.extend(title.split())

            # count word frequency
            counts = Counter(words)
            store_json(counts, p('words.json'))


def make_graph_files():
    # words.json -> words.js, the file d3_graph.html loads
    with open(p('words.json')) as f:
        words = json.load(f)
    with open(p('words.js'), 'w', encoding='utf-8') as f:
        f.write('scores = ' + json.dumps(words, ensure_ascii=False))
    print('wrote file: ' + p('words.js'))


default_args = {
    'owner': 'Rawan',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['rawan.mft@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}  
   
with DAG(
   "word_frequency_pipeline",
   description="Scrape pages, clean the text, count words and prepare the D3 chart data",
   tags=["etl", "d3"],
   start_date=days_ago(1),
   schedule_interval="@daily",
   catchup=False,
) as dag:
    
#BS4 is installed in Dockerfile to make sure it installs once only

   # ts are tasks

   t1 = PythonOperator(
       task_id='task_one',
       depends_on_past=False,
       python_callable=catalog
   )
    
   t2 = PythonOperator(
    task_id='task_two',
    depends_on_past=False,
    python_callable=combine
                    )
   
   t3 = PythonOperator(
    task_id='task_three',
    depends_on_past=False,
    python_callable=titles
                    )
   
   t4 = PythonOperator(
    task_id='task_four',
    depends_on_past=False,
    python_callable=clean
                    )
   
   t5 = PythonOperator(
    task_id='task_five',
    depends_on_past=False,
    python_callable=count_words
                    )
   
   t6 = PythonOperator(
    task_id='task_six',
    depends_on_past=False,
    python_callable=make_graph_files
                    )

   t1>>t2>>t3>>t4>>t5>>t6