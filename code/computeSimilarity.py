import sys
import re
import math
import igraph
from textblob import TextBlob
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from itertools import islice

sc = SparkContext("local", "Compute-News-Article-Similarity")
sqlContext = SQLContext(sc)
sc.setLogLevel('ERROR')

split_regex = r'\W+'  #Regex for Any Non-alphanumeric character

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))
 
def simpleTokenizeString(string):
    """ String tokenization function which tokenizes the given string as per the Regex
    Args:
        string (str): input string
    Returns:
        list: a list of tokens
    """
    return filter(len, re.split(split_regex, string))

def tokenizeString(string):
    """ String tokenization function that excludes stopwords
    Args:
        string (str): input string
    Returns:
        list: a list of tokens without stopwords
    """
    return filter(lambda x: x not in stopwords, simpleTokenizeString(string))

def tokenFrequency(tokens):
    """ Compute Term Frequency for each document
    Args:
        tokens (list of str): input list of tokens from tokenizeString
    Returns:
        dictionary: a dictionary of tokens to its TF values
    """
    tokenCounts = {} 
    for t in tokens: 
      tokenCounts[t] = tokenCounts.get(t, 0) + (1.0/len(tokens))
    return tokenCounts
  
def inverDocFreq(corpus):
    """ Compute Inverse Document Frequency for each token
    Args:
        corpus (RDD): input corpus
    Returns:
        RDD: a RDD of (token, IDF value)
    """
    uniqueTokens = corpus.flatMap(lambda x: list(set(x[1])))
    tokenCountPairTuple = uniqueTokens.map(lambda x: (x, 1))
    tokenSumPairTuple = tokenCountPairTuple.reduceByKey(lambda a,b: a+b)
    N = corpus.count()
    return (tokenSumPairTuple.map(lambda x: (x[0], float(N)/float(x[1]))))
  
def computeTFIDF(tokens, inverDocFreq):
    """ Compute TF-IDF - Product of Token Frequency and Inverse Document Frequency for each token
    Args:
        tokens (list of str): input list of tokens from tokenizeString
        inverDocFreq (dictionary): record to IDF value
    Returns:
        dictionary: a dictionary of records to TF-IDF values
    """
    tfs = tokenFrequency(tokens)
    tfIdfDict = {k: v*inverDocFreq[k] for k, v in tfs.items()}
    return tfIdfDict

def dotProductCalc(a, b):
    """ Compute dot product between two documents represented as vector of their TF-IDF weights
    Args:
        a (dictionary): first dictionary of record to value
        b (dictionary): second dictionary of record to value
    Returns:
        dotProd: result of the dot product with the two input dictionaries
    """
    return sum(a[k]*b[k] for k in a if k in b)

def magnitude(a):
    """ Compute square root of the dot product
    Args:
        a (dictionary): a dictionary of record to value
    Returns:
        magnitude (float): the square root of the dot product value
    """
    return math.sqrt(sum(a[k]**2 for k in a))

def cosineSimilarityFormula(a, b):
    """ Compute cosine similarity
    Args:
        a (dictionary): first dictionary of record to value
        b (dictionary): second dictionary of record to value
    Returns:
        cosineSimilarityFormula: dot product of two dictionaries divided by the magnitude of the first dictionary and
        			 then by the magnitude of the second dictionary
    """
    if(float(magnitude(a)*magnitude(b)) > 0):
        return float(dotProductCalc(a, b))/float(magnitude(a)*magnitude(b))
    else:
        return float(0)
  
def computeCosineSimilarity(string1, string2, idfsDictionary):
    """ Compute cosine similarity between two documents
    Args:
        string1 (str): first document
        string2 (str): second document
        idfsDictionary (dictionary): a dictionary of IDF values
    Returns:
        cosineSimilarityFormula: cosine similarity value
    """
    w1 = computeTFIDF(tokenizeString(string1),idfsDictionary)
    w2 = computeTFIDF(tokenizeString(string2),idfsDictionary)
    if(w1>0 and w2>0):
      return cosineSimilarityFormula(w1, w2)
  
def calcSimilarity(record):
    """ Compute similarity on a combination of news article
    Args:
        record: a pair, (Article 1, Artice 2)
    Returns:
        pair: a pair, (Article 1 URL, Article 2 URL, cosine similarity value)
    """
    hinduRec = record[0]
    ieRec = record[1]
    hinduID = hinduRec[0]
    ieID = ieRec[0]
    hinduValue = hinduRec[1]
    ieValue = ieRec[1]
    cs = computeCosineSimilarity(hinduValue, ieValue, idfsWeights)
    return (hinduID, ieID, cs)
  
#-------------------------------------------------------------------------------------------------------------------------------
# Read The Hindu, The Indian Express and stopwords file
#-------------------------------------------------------------------------------------------------------------------------------

print "Read The Hindu, The Indian Express and stopwords file"
IEdf = sqlContext.read.load('file:///home/cloudera/sparkproj/data/ie.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
hindudf = sqlContext.read.load('file:///home/cloudera/sparkproj/data/hindu.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
stopwords = set(sc.textFile('file:///home/cloudera/sparkproj/data/stopwords.txt').collect())

#-----------------------------------------------------------------------------------------
# Creating News Articles Dictionary
#-----------------------------------------------------------------------------------------
print "Creating News Articles Dictionary"
hinduDataDict = hindudf.map(lambda p: ((p.id).encode('utf-8'),[(p.title).encode('utf-8'),(p.link).encode('utf-8'),(p.pubDate).encode('utf-8')])).collectAsMap()
ieDataDict = IEdf.map(lambda p: ((p.id).encode('utf-8'),[(p.title).encode('utf-8'),(p.link).encode('utf-8'),(p.pubDate).encode('utf-8')])).collectAsMap()
newsArticleDict = dict(hinduDataDict, **ieDataDict)

# To print the merged dictionary
#from pprint import pprint
#pprint(newsArticleDict)

#-----------------------------------------------------------------------------------------
# Computing Similarity on the basis of Title, Creating RDD of news article ID and Ttile
#-----------------------------------------------------------------------------------------

print "Computing Similarity on the basis of Title, Creating RDD of news article ID and Ttile"
hinduRDD = hindudf.map(lambda p: (str(p.id),str(p.title)))
IERDD = IEdf.map(lambda p: (str(p.id),str(p.title)))

#-----------------------------------------------------------------------------------------
# Tokenising the news article
#-----------------------------------------------------------------------------------------

print "Tokenising the news article"
hinduRecToToken = hinduRDD.map(lambda x: (x[0], tokenizeString(x[1])))
ieRecToToken = IERDD.map(lambda x: (x[0], tokenizeString(x[1])))

# print hinduRecToToken.take(5)
# print ieRecToToken.take(5)

#-----------------------------------------------------------------------------------------
# Corpus containing all tokens in all articles from both the source
#-----------------------------------------------------------------------------------------

print "Creating Corpus containing all tokens in all articles from both the source"
corpusRDD = hinduRecToToken.union(ieRecToToken)

#-----------------------------------------------------------------------------------------
# Computing IDF of each token in the corpus
#-----------------------------------------------------------------------------------------

print "Computing IDF of each token in the corpus"
idfsWeights = inverDocFreq(corpusRDD).collectAsMap()

#To print n items of the dictionary
nitems = take(20, idfsWeights.iteritems())
print nitems

#------------------------------------------------------------------------------------------------------------
# Computing Cartesian product of each news article from source 1 with each news article for source 2.
# Cartesian Product will compute tuples of news articles which is then used to calculate the similarity
#-------------------------------------------------------------------------------------------------------------

print "Computing Cartesian product of each news article from source 1 with each news article for source 2"
crossProduct = hinduRDD.cartesian(IERDD).cache()

#-----------------------------------------------------------------------------------------
# Computing Similarity between each news article from source 1 wth news article from source 2
#-----------------------------------------------------------------------------------------

print "Computing Similarity between each news article from source 1 with news article from source 2"
similarities = crossProduct.map(calcSimilarity)
similarities.saveAsTextFile('file:///home/cloudera/sparkproj/data/similarityscores')

#-----------------------------------------------------------------------------------------
# Find similar News Articles having similarity score greater than 0.7
#-----------------------------------------------------------------------------------------

results = similarities.filter(lambda record: record[2] > 0.7).takeOrdered(20, lambda s: -s[2])

#-----------------------------------------------------------------------------------------
# Compute Groups of similar News Articles present in the entire data set using igraph package 
#-----------------------------------------------------------------------------------------

print "Printing similar News Articles having similarity score greater than 0.7"

# build the graph object
g = igraph.Graph()
edges, vertices = set(), set()
for e in results:
  vertices.update(e[:2])
  edges.add(e[:2])

g.add_vertices(list(vertices))
g.add_edges(list(edges))
# decompose the graph into sub graphs based on vertices connection
results = [[v['name'] for v in sg.vs()] for sg in g.decompose(mode="weak")]

print " -----------------------------------------------------------------------------------------"
print " The Matching News Articles from the entire data set (Top 20 Results) : "
print " -----------------------------------------------------------------------------------------"

for result in results:
  newsgroup = result
  count = 1
  for group in newsgroup:
    print str(count) + ". " + str(newsArticleDict[group][0]) + ' - ' + str(newsArticleDict[group][1]) + ' - ' + str(newsArticleDict[group][2])
    count = count + 1
    
  print "-------------------------------------------------------------------------------------------"
  print " "
  
#---------------------------------------------------------------------------------------------------------------------------------------
# Compute Groups of similar News Articles present in the entire data as per the User Input Search String
# Display the computed results as per the Sentiment of the News Article as Positive, Negative or Neutral, computed using TextBlob package
#--------------------------------------------------------------------------------------------------------------------------------------

print " "
print "Compute Groups of similar News Articles present in the entire data as per the User Input Search String"
print "Display the computed results as per the Sentiment of the News Article as Positive, Negative or Neutral"

if (len(sys.argv) > 1):

    user_input = str(sys.argv[1])
    userstring = sc.parallelize([('user',user_input)])
    corpusRDD2 = hinduRDD.union(IERDD)
    userCrossSmall = (userstring.cartesian(corpusRDD2))
    userSimilarities = (userCrossSmall.map(calcSimilarity))
    
    results = userSimilarities.filter(lambda record: record[2] > 0.01).takeOrdered(20, lambda s: -s[2])
    resultID = []

    for result in results:
      resultID.append(result[0])
      resultID.append(result[1])

    resultID = set(resultID)

    print " -----------------------------------------------------------------------------------------"
    print " The Matching News Articles for the input '" + str(user_input) + "' are: "
    print " -----------------------------------------------------------------------------------------"


    polarPositive = []
    polarNeutral = []
    polarNegative = []

    for result in resultID:
      if not result.startswith('user'):
     
        c = TextBlob(str(newsArticleDict[result][0]))
        sentiment = c.sentiment.polarity

        if sentiment > 0.1:
          polarPositive.append(result)
        elif sentiment < -0.1:
          polarNegative.append(result)
        else:
          polarNeutral.append(result)

    def printNewsArticle(newsList):
      count = 1
      for news in newsList:
        print str(count) + ". " + str(newsArticleDict[news][0]) + ' - ' + str(newsArticleDict[news][1])
        count = count + 1
  
    if len(polarPositive) > 0:
      print "Matching News Articles - Positive Sentiments"
      printNewsArticle(polarPositive)
  
    if len(polarNegative) > 0:
      print " "
      print " -----------------------------------------------------------------------------------------"
      print "Matching News Articles - Negative Sentiments"
      printNewsArticle(polarNegative)

    if len(polarNeutral) > 0:
      print " "
      print " -----------------------------------------------------------------------------------------"
      print "Matching News Articles - Neutral Sentiments"
      printNewsArticle(polarNeutral)


# spark-submit --jars /home/cloudera/sparkproj/jars/spark-csv_2.11-1.5.0.jar,/home/cloudera/sparkproj/jars/commons-csv-1.4.jar /home/cloudera/sparkproj/code/computeSimilarity.py 'what is demonetisation'