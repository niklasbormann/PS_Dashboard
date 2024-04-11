import json
import pandas as pd
import re
from nltk.corpus import stopwords

# client = Elasticsearch(hosts='https://localhost:9200',
#                      basic_auth=("elastic", "GML0OBa-2=sjGhskAf*_"),
#                     verify_certs=True,
#                    ca_certs=r'C:\Users\Nikla\Downloads\elasticsearch-8.13.1-windows-x86_64\elasticsearch-8.13.1\config\certs\http_ca.crt'
#                   )

# searchresult = client.search(index='blueskydata',  body={})

# df = Select.from_dict(searchresult).to_pandas()

testdata = open('Dashboardtestdata.json')

doc = json.load(testdata)

df = pd.DataFrame(doc)

df.drop_duplicates(subset=['Text'])

# Normalisierung

df['Text'] = df['Text'].str.lower()
df['Text'] = df['Text'].str.strip()
df['Text'] = df['Text'].str.encode('utf-8')

# Entfernen von Sonderzeichen
df['Text'] = df['Text'].str.replace(r'[^\w\s]', '', regex=True)

# Entfernen von Stoppwörtern
df['Text'] = df['Text'].apply(lambda x: ' '.join([word for word in x.split() if word not in stopwords.words('english')]))

# Aufspalten von angehängten Wörtern
df['Text'] = df['Text'].apply(lambda x: ' '.join(re.sub('([a-z])([A-Z])', r'\1 \2', x).split()))

# Entfernen von URLs und Hyperlinks
df['Text'] = df['Text'].apply(lambda x: re.sub(r'^https?://.*[\r\n]*', '', x, flags=re.MULTILINE))

print(df['Text'])
