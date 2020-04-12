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
Es muy importante hacer las peticiones a las webs de forma gradual y sin concentrar
las peticiones en un periodo corto de tiempo. Por ello, usamos un sleep en cada
llamada a la funcion que hace peticiones.
En caso de no poner esta restriccion, estariamos saturando la web de un modo similar
a como se hacen los ataques DDoS. 
El usuario es el unico responsable de cómo use este código.
'''
__ELASTIC_FACTOR__ = 5


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
url_parse = udf(url_parse_, url_struct)

def scrape_in_(netloc, url):
    time.sleep(random.random()*__ELASTIC_FACTOR__) 
    try:
        content = requests.get(url)
        content = BeautifulSoup(content.text, 'lxml')
        hrefs = [href['href'] for href in content.find_all(href = True)]
        return list(set([href for href in hrefs if netloc in href]))
    except:
        return []

def scrape_out_(netloc, url):
    time.sleep(random.random()*__ELASTIC_FACTOR__) 
    try:
        content = requests.get(url)
        content = BeautifulSoup(content.text, 'lxml')
        hrefs = [href['href'] for href in content.find_all(href = True)]
        return list(set([href for href in hrefs if netloc not in href]))
    except:
        return []

def scrape_all_(netloc, url):
    time.sleep(random.random()*__ELASTIC_FACTOR__) 
    try:
        content = requests.get(url)
        content = BeautifulSoup(content.text, 'lxml')
        hrefs = [href['href'] for href in content.find_all(href = True)]
        return list(set(hrefs))
    except:
        return []

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










if __name__ == '__main__':
    with SparkSession.builder.master('local[4]').getOrCreate() as spark:
        # urls = spark.read.json('urls.json')
        # urls.show()

        # master_info = urls \
        #     .withColumn('info', url_parse('url')) \
        #     .select(
        #         'url',
        #         'info.scheme',
        #         'info.netloc',
        #         'info.path',
        #         'info.parameters',
        #         'info.query',
        #         'info.fragment'
        #     )

        # master_info.show()

        # urls = urls \
        #     .join(
        #         master_info.select('url', 'netloc'),
        #         on = ['url'],
        #         how = 'left'
        #     ) \
        #     .withColumn('href', explode_outer(scrape_in('netloc', 'url')))
        
        # urls.show(truncate = False)


        
        # master, master_info = expand(urls, master_info)
        # master.show(n = 1000, truncate = False)
        # master_info.show(n = 1000, truncate = False)

        # master.write.format('json').save('master')
        # master_info.write.format('json').save('master_info')
        
        master = spark.read.json('master')
        master_info = spark.read.json('master_info')
        
        #master.show()

        nodes = master \
            .select('url') \
            .dropDuplicates()

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


        # Now create a graph and plot it
        G = nx.Graph()
        G.add_nodes_from(nodes)
        G.add_edges_from(edges)
        
        print(G.number_of_nodes())
        print(G.number_of_edges())

        node_info_list = master_info \
            .select('url', 'path') \
            .rdd

        node_url = node_info_list \
            .map(lambda x: x['url']) \
            .collect()

        node_path = node_info_list \
            .map(lambda x: x['path']) \
            .collect()

        
        node_info = dict(zip(node_url, node_path))

        colors = nx.degree_centrality(G).values()

        
        
        nx.draw_spring(
            G,
            with_labels = True,
            node_size = 10,
            font_size = 5,
            labels = node_info,
            edge_color = 'gray',
            style = 'dashed',
            width = 0.1,
            node_color = list(colors)
        )
        
        plt.show()
