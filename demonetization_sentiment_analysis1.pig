


/*Loading the csv file using pig*/

load_tweets = LOAD '/input_files/demonetization-tweets.csv' USING PigStorage(',');




/* Extracting the needed data.... */
/* $0 represents first field and $1 represents the second field.... */
/* "id" is the alias name of $0 and "text" is the alias name of $1 */

extract_details = FOREACH load_tweets GENERATE $0 as id,$1 as text;




 /* The following characters are considered to be word separators: space, double quote("), coma(,) parenthesis(()), star(*).The following characters are considered to be word separators: space, double quote("), coma(,) parenthesis(()), star(*). */

/* TOKENIZE will split the records based on above seperators and give the bag of words.... */

/* FLATTEN will remove the parenthesis like () and {} */

/* Below code of FLATTEN(TOKENIZE(text)) will remove "{}" from the bag of words and tokens i.e. words from each record will get associate with each record as 3rd field in a iterative manner until the tokens will be finished   */

tokens = foreach extract_details generate id,text, FLATTEN(TOKENIZE(text)) As word;




/* For checking the affect of step 'tokens', use the below commands */

tokens_limit = LIMIT tokens 8;
dump tokens_limit;



/* Loading the dictionary which have rating for words */

dictionary = LOAD '/input_files/AFINN.txt' using PigStorage('\t') As(word:chararray,rating:int);




/* 'replicated' join is the special type of join in which second relation is small enough to fit into the main memory which will help in efficient join.... */

word_rating = join tokens by word left outer, dictionary by word using 'replicated';




/* This will give sample data i.e. first 3 rows of word_rating */

word_rating_limit = limit word_rating 3;
dump word_rating_limit;



/* We are iteratively selecting the data from the relation "word_rating" and selecting fields from respective relations */
/* To select the field which is the part of a certain relation we need to use  double colon "::" as below in case of some operation has already been applied between two relations like in previous steps */

rating = foreach word_rating generate tokens::id as id,tokens::text as text, dictionary::rating as rate;



/* This will give sample data i.e. first 3 rows of rating */

rating_limit = limit rating 3;



/* This below command will group relations "rating" on the basis of id and text combinedly */

word_group = group rating by (id,text);



/* First 2 rows of word_group */

word_group_limit = limit word_group 2;




/* This line is crucial in deciding the rating for any tweet on twitter data as it will sum up all the word's rating in a tweet  */
/* Below line will generate group which is from relation word_group and as all the data will get group on the basis of (id,text) we can perform average of those tokens also as per their ratings */

avg_rate = foreach word_group generate group, AVG(rating.rate) as tweet_rating;



/* First 100 rows of avg_rate */

avg_rate_limit = limit avg_rate 100; 

dump avg_rate;



/* Filter the positive tweets*/

positive_tweets = filter avg_rate by tweet_rating >= 0;



/* Filter the negative tweets*/

negative_tweets = filter avg_rate by tweet_rating < 0;



/* Storing the positive and negative tweets output in output_files folder*/

store positive_tweets into '/output_files/positive_tweets_output1';
store negative_tweets into '/output_files/negative_tweets_output1';