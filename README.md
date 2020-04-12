Disclousure
This little project is intended to show uncommon examples of pyspark.
The web pages used in the project are merely examples, 
with no intention of causing damage or misuse of its services. 
The author is not responsible for the use that users may give to this program, 
being exempt from any execution and damage that third parties may cause.
The content displayed on the websites is the exclusive property of their authors. 
The fact of showing it in this project is for didactic purposes only.

# pyspark

Some little examples using Spark.


We are going to construct a table with the next structure:

url | href
----|-----
www.example.com | www.example.com/page=1
www.example.com | www.example.com/about_us
...             | ...

Then we have pairs 
(www.example.com, www.example.com/page=1),
(www.example.com, www.example.com/about_us), ...

which can be seen as edges in a graph.
