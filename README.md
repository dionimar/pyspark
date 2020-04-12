## Disclousure
This little project is intended to show uncommon examples of pyspark.
The web pages used in the project are merely examples, 
with no intention of causing damage or misuse of its services. 
The author is not responsible for the use that users may give to this program, 
being exempt from any execution and damage that third parties may cause.
The content displayed on the websites is the exclusive property of their authors. 
The fact of showing it in this project is for didactic purposes only.

# Required libraries

You can install dependencies with 

```bash
pip3 install --user bs4 urllib requests networkx matplotlib pyspark
```

# How data looks like

We are going to construct a table with the next structure:

url | href
----|-----
https://www.example.com | https://www.example.com/page=1
https://www.example.com | http://www.example.com/about_us
...             | ...

Then we have pairs 
* (https://www.example.com, https://www.example.com/page=1)
* (https://www.example.com, http://www.example.com/about_us)
* ...

which can be seen as edges in a graph.

Meanwhile we are going to track each url info and store it in a table:

url | scheme | netloc | path | parameters | query | fragment
----|--------|--------|------|------------|-------|---------
http://www.example.com/about_us | http | www.example.com | /about_us | | | 
https://www.example.com/page=1 | https | www.example.com | /page=1 | | |

You can read more about url parsing in [urllib](https://docs.python.org/3/library/urllib.parse.html#module-urllib.parse).

Then we can choose *netloc* as domain to restrict graph scope, which is  done in scrape_ functions:
```python
def scrape_in_(netloc, url):
'''
Random sleep for distribute requests over time (avoid DDoS)
Take care of table size: more registers requires more time (increase __ELASTIC_FACTOR)
'''
  time.sleep(random.random() * __ELASTIC_FACTOR
  content = requests.get(url)                                      # Get html code from url
  content = BeautifulSoup(content.text, 'lxml')                    # Parse html content. Tree representation from labels.
  hrefs = [href['href'] for href in content.find_all(href = True)] # List of all href labels
  return list(set([href for href in hrefs if netloc in href]))     # Returns the minimal list of hrefs which contains its domain
```
Refer to [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/) for further reading.

# Main functionality

Once we have the first table (url, href) we can construct a new table with previous href as url, and scrape the new urls to obtain its hrefs. We can repeat it as much as we want, so growing the graph. 
As said before, we have the pairs (url, href) which are in fact edges from a directed graph, with all url's and href's as nodes.
That's what we do in the **main** function.

We construct the graph with

```python
# import networkx as nx
G = nx.Graph()
G.add_nodes_from(nodes)
G.add_edges_from(edges)
```

and add some drawing options with

```python
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
```


# Issues

There is an issue with url naming: all urls are mapped to its path for naming, which yields an error:
path for the next urls match, so appears as a single cluster in the graph
* www.example.com/ --->>> /
* www.domain.com/  --->>> /

All your ideas for solving the issue are welcome!
Feel free to do whatever you want with the code, it's been done with sharing and freedom in mind.


# Contact
Contact me at dnmrtnz94 [at] gmail.
Thanks for reading!!
