## Disclousure
This little project is intended to show uncommon examples of pyspark.
The web pages used in the project are merely examples, 
with no intention of causing damage or misuse of its services. 
The author is not responsible for the use that users may give to this program, 
being exempt from any execution and damage that third parties may cause.
The content displayed on the websites is the exclusive property of their authors. 
The fact of showing it in this project is for didactic purposes only.

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

Then we can choose *netloc* as domain to restrict graph scope:
* scrape_in
* scrape_out
* scrape_all


# Main functionality

Once we have the first table (url, href) we can construct a new table with previous href as url, and scrape the new urls to obtain its hrefs. We can repeat it as much as we want, so growing the graph. 
As said before, we have the pairs (url, href) which are in fact edges from a directed graph, with all url's and href's as nodes.
That's what we do in the **main** function.
