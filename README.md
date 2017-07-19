# Text Analytics and Entity Resolution Using Apache Spark

There is a vast and rapidly increasing quantity of online news content, each source publishing or covering almost the same news - content wise. Since these articles or entities do not share any common attribute but still have an underlying relationship, there is inherently the problem of identifying and linking or grouping different data content of the same real world entity. One such solution to this problem can be resolved using Entity Resolution. 

Entity Resolution, or "Record linkage" refers to the process of joining records from one data source with another that describe the same entity. Entity Resolution (ER) refers to the task of finding records in a dataset that refer to the same entity across different data sources – in this case news articles from two major Indian news channel, Indian Express and The Hindu. The task of resolving entities and detecting relationships becomes easy with ER, particularly when combining two datasets may or may not share a common identifier.

The objective of this report is to present the result of how Apache Spark can be used to apply powerful and scalable text analysis techniques and perform entity resolution across two datasets of news articles. The report highlights the process of data collection and cleaning, using Entity Resolution technique display similar news articles as group and also display similar news articles as per the search query input from the user, and finally divide the results as per the sentiment carried by the respective news article.

Keywords – Apache Spark, Text analytics, Entity Resolution, Sentiment Analysis 
