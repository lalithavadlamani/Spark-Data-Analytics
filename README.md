# Spark Data Analytics

## Users with similar interest and User recommendations

### Dataset description 

<p align = "justify"> <a href="https://github.com/lalithavadlamani/Spark-Data-Analytics/blob/main/tweets.json"> Dataset </a> used contains tweet objects downloaded from Twitter using <a href="https://developer.twitter.com/en/docs/twitter-api/tweets/search/introduction"> Tweeter standard search API </a>.The downloaded tweet objects are stored in a single JSON file. A tweet object may refer to a general tweet, a retweet or a reply to a tweet. A general tweet is "a message posted to Twitter containing text, photos, a GIF, and/or video". A reply is "a response to another person’s tweet". A Retweet is "a re-posting of a Tweet". </p>
<p align = "justify">
The common fields in all tweet objects are: 
  
* id: the unique id of the tweet.
* created at: the date and time the tweet is created.
* text: the textual content of the tweet
* user id: the id of the user who created the tweet.
* retweet count: retweet count of the tweet or its parent tweet if the tweet is a retweet
* favorite count: favorite count of the tweet

The optional fields are:

* retweet id: included only in retweet object. It contains the id of the tweet it re-posts.
* retweet user id: included only in retweet object. It contains the user id of the
tweet it re-posts.
* replyto id: included only in reply object. It contains the id of the tweet it responds to.
* replyto userid: included only in reply object. It contains the user id of the tweet
it responds to
* user mentions: included only if the tweet text mentions one or more users. It is an
array of fid, indicesg showing the id of the users mentioned in the tweet text, as well as the location this user is mentioned.
* hash tags: included only if the tweet text contains one or more hash tags. It is an
array of ftag, indicesg showing the hash tag appearing in the tweet text and the
location it appears
</p>

### Users with similar interest 
<p align = "justify">
The workload <a href="https://github.com/lalithavadlamani/Spark-Data-Analytics/blob/main/Users%20with%20similar%20interest.ipynb"> Users with similar interest.ipynb </a> is a spark application which gives top 5 users with similar interest in tweets for a given user id. Users interest is measured based on the tweets they have replied or retweeted. 
</p>

<p align = "justify">
For each user a document representation has been generated with the ids of the tweets the user has replied or retweeted. For instance, user u1 has retweeted t1, t4 and replied t3,t4, the document representation is (t1,t3,t4,t4). Each user document representation is then transformed into a vector and Cosine similarity has been used to compute similarity between two user vectors. Two Feature extractors, TF-IDF and Word2Vec from SparkML API, have been experimented to carry out the task. </p>

<p align = "justify"> <strong>Data formatting and Preparation</strong>- Raw data has been prepared according to the format required as an input to perform tfidf and word2vec. This included selecting only the required columns and forming a document representation for each user which has the tweet ids of retweeted and replied tweets by the user. 
</p>

<p align = "justify">
For tfidf, HashingTF of pyspark ml has been used to calculate the term frequency with the parameter numFeatures set to 16384 followed by an idf model to extract the feature representation for each user. Random user has been selected and 5 similar users were found for the selected user by calculating the cosine similarity using the feature representation. </p>

<p align = "justify">
For word2vec, Word2Vec of pyspark ml has been used to build a model with parameters vectorSize and minCount set to 15 and 1 respectively. Data has been transformed using this model to extract feature for each user. Similar to tfidf, cosine similarity has been calculated with a random user selected from the data. </p>

<p align = "justify">
The final outputs of both the feature extractors followed by cosine similarity return the top 5 users with similar interest to the randomly selected user. </p>

### User recommendation
<p align = "justify">
Two groups of users have been considered for this task. One group contains users that appear in the user id field of a tweet object, labelled as tweet users; another group contains users mentioned in a tweet, labelled as mention users. The workload <a href = "https://github.com/lalithavadlamani/Spark-Data Analytics/blob/main/User%20recommendations.ipynb"> User recommendations.ipynb </a> uses collaborative filtering algorithm in SparkML library to recommend top 5 mention users to each tweet user in the data set. Collaborative filtering aims to fill in the missing entries of a user-item association matrix. Tweet users are treated as ‘user’ and mention users as ‘item’ in this setting. The rating is computed as number of times a tweet user mentions a mention user.
</p>
<p align = "justify"> <strong>Data formatting and Preparation</strong>- Raw data has been prepared according to the format required as an input to perform collaborative filtering. This included selecting only the required columns and adding a rating column which is the number of times a tweet user has mentioned the mentioned user. Users who tweeted are ‘tweet users’ and users who have been mentioned by tweet users are considered as ‘mention users’. Since the user ids of tweet users and mention users are very large numbers they go out of range of the accepted integer value range so they have been mapped to indices within the integer range. The output of all the formatting and transformation is a user-item association matrix which is considered the required input to perform collaborative filtering.
</p>
<p align = "justify">
For collaborative filtering, Alternating Least Squares(ALS) algorithm of pyspark ml has been used. The input for it should be in the integer value range (the reason we mapped the values in the data preparation stage). The ALS model has been built with the following parameters rank=8, maxIter=20, regParam=0.01, implicitPrefs=True, coldStartStrategy=Drop and the inputs required – users, items and ratings.
</p>
<p align = "justify">
The purpose of the workload was to recommend 5 mention users(which is the items) for each tweet user hence the function recommendForAllUsers has been used. Since we mapped the initial user id values to integer range values they have been re-mapped to the original id values to show the final output required.</p>
