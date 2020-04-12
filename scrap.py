from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse
import time
import random
import networkx as nx
import matplotlib.pyplot as plt


'''
It is very important to make requests to the websites gradually and without concentrating
requests in a short period of time. Therefore, we use a sleep in each
requests function.
In case of not putting this restriction, we would be saturating the web in a similar way
to how DDoS attacks are made.
The user is solely responsible for how they use this code.
'''


__ELASTIC_FACTOR = 5


'''
urllib.parse.urlparse (parsing method for url's) returns a tuple with the following
fields. You can see it in a pyton shell:

>>> from urllib.parse import urlparse
>>> urlparse('https://docs.python.org/3/library/urllib.parse.html#module-urllib.parse')
ParseResult(
    scheme='https', 
    netloc='docs.python.org', 
    path='/3/library/urllib.parse.html', 
    params='', 
    query='',  
    fragment='module-urllib.parse'
)

'''
url_struct = StructType([
    StructField('scheme',     StringType()),
    StructField('netloc',     StringType()),
    StructField('path',       StringType()),
    StructField('parameters', StringType()),
    StructField('query',      StringType()),
    StructField('fragment',   StringType())
])

def url_parse_(url):
    return urlparse(url)
url_parse = udf(url_parse_, url_struct) # Define the output format


'''
Next three functions main goal is to download the html content of an url and
extract the href labels for further processing.
scrape_in_  -> drops non domain related hrefs,
scrape_out_ -> drops domain related hrefs and
scrap_all_  -> does not drop anything.
'''
def scrape_in_(netloc, url):
    time.sleep(random.random()*__ELASTIC_FACTOR) 
    try:
        content = requests.get(url)
        content = BeautifulSoup(content.text, 'lxml')
        hrefs = [href['href'] for href in content.find_all(href = True)]
        return list(set([href for href in hrefs if netloc in href]))
    except:
        return []

def scrape_out_(netloc, url):
    time.sleep(random.random()*__ELASTIC_FACTOR) 
    try:
        content = requests.get(url)
        content = BeautifulSoup(content.text, 'lxml')
        hrefs = [href['href'] for href in content.find_all(href = True)]
        return list(set([href for href in hrefs if netloc not in href]))
    except:
        return []

def scrape_all_(netloc, url):
    time.sleep(random.random()*__ELASTIC_FACTOR) 
    try:
        content = requests.get(url)
        content = BeautifulSoup(content.text, 'lxml')
        hrefs = [href['href'] for href in content.find_all(href = True)]
        return list(set(hrefs))
    except:
        return []

# All functions return a single array with href links.
scrape_in  = udf(scrape_in_,  ArrayType(StringType()))
scrape_out = udf(scrape_out_, ArrayType(StringType()))
scrape_all = udf(scrape_all_, ArrayType(StringType()))


def expand(previous, previous_info):
    new = previous \
        .withColumn('url', col('href')) \
        .drop('href') \
        .dropDuplicates()
    
    new_index = new.select('url') \
        .subtract(
            previous.select('url')
        )
    new = new_index \
        .join(
            new,
            ['url'],
            how = 'left'
        ) \
        .withColumn('href', explode_outer(scrape_all('netloc', 'url'))) \
        .union(previous)

    new_info = new_index \
        .union(new.select('href').withColumnRenamed('url', 'href')) \
        .withColumn('info', url_parse('url')) \
        .select(
            'url',
            'info.scheme',
            'info.netloc',
            'info.path',
            'info.parameters',
            'info.query',
            'info.fragment'
        ).union(previous_info)
    
    return new, new_info



def acquire_data(spark, seedlist, filename_out):
    # Read the seed list (list of inital domains)
    urls = spark.read.json(seedlist)
    # Parse all url (extract domain, http(s),...)
    master_info = urls \
        .withColumn('info', url_parse('url')) \
        .select(
            'url',
            'info.scheme',
            'info.netloc',
            'info.path',
            'info.parameters',
            'info.query',
            'info.fragment'
        )
    
    master_info.show()

    # Join our urls with netloc (domain) from master_info (scrape_** needs a domain)
    urls = urls \
        .join(
        master_info.select('url', 'netloc'),
            on = ['url'],
            how = 'left'
        ) \
        .withColumn('href', explode_outer(scrape_in('netloc', 'url')))
    urls.show(truncate = False)
    
    # One level expansion (following hrefs)
    master, master_info = expand(urls, master_info)
    master.show(n = 1000, truncate = False)
    master_info.show(n = 1000, truncate = False)
    # Save the data. We recommend to save it once its been properlly downloaded
    # That way you don't saturate the net
    master.write.format('json').save(filename_out)
    master_info.write.format('json').save(filename_out + '_info')
    return master, master_info





if __name__ == '__main__':
    with SparkSession.builder.master('local[4]').getOrCreate() as spark:
        
        # master, master_info = acquire_data(
        #     spark,
        #     seedlist = 'urls.json',
        #     filename_out = 'master'
        # )

        ## In case you saved the data, read it:
        master = spark.read.json('master')
        master_info = spark.read.json('master_info')


        # Create nodes and edges from url and href columns
        nodes = master \
            .select('url') \
            .dropDuplicates()

        # All nodes cames from 'url' and 'href' columns.
        nodes = nodes \
            .union(master \
                   .select('href') \
                   .withColumn('url', col('href')) \
                   .drop('href') \
                   .dropDuplicates()) \
            .rdd \
            .map(lambda x: x['url']) \
            .collect()
        
        edges = master \
            .select('url', 'href') \
            .rdd \
            .map(lambda x: (x['url'], x['href'])) \
            .collect()

        # We set the url path as node name.
        node_info_list = master_info \
            .select('url', 'path') \
            .rdd

        node_url = node_info_list \
            .map(lambda x: x['url']) \
            .collect()

        node_path = node_info_list \
            .map(lambda x: x['path']) \
            .collect()

        # Now create a graph and plot it
        G = nx.Graph()
        G.add_nodes_from(nodes)
        G.add_edges_from(edges)
        
        print(G.number_of_nodes())
        print(G.number_of_edges())

        node_info = dict(zip(node_url, node_path))
        colors = nx.degree_centrality(G).values()
        nx.draw_spring(
            G,
            with_labels = True,
            node_size   = 10,
            font_size   = 5,
            labels      = node_info,
            edge_color  = 'gray',
            style       = 'dashed',
            width       = 0.1,
            node_color  = list(colors)
        )
        plt.show()
