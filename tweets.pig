--Load data
REGISTER '/share/lib/elephant-bird-pig-4.1.jar';
REGISTER '/share/lib/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/share/lib/google-collections-1.0.jar';
REGISTER '/share/lib/json-simple-1.1.jar';
REGISTER '/usr/lib/hadoop/lib/hadoop-lzo-cdh4-0.4.15-gplextras.jar';
REGISTER '/opt/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/pig/pig-0.11.0-cdh4.4.0.jar';

REGISTER 'lib/elephantbird/lib/elephant-bird-mahout-4.1.jar';
REGISTER 'lib/mahout-distribution-0.7/mahout-core-0.7.jar';
REGISTER 'lib/mahout-distribution-0.7/mahout-math-0.7.jar';

--REGISTER ToSet.jar;
REGISTER 'ToSet.py' using org.apache.pig.scripting.jython.JythonScriptEngine as CSV;

-- load data
tweets = LOAD '/data/twitter/streams/prometheus/' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (tweet:map[]);

-- extract tweet ids and entities
id_tweets = FOREACH tweets GENERATE  (CHARARRAY)tweet#'user'#'id_str' AS id_str, (CHARARRAY)tweet#'text' AS tweets;

-- removing null values
id_tweets_clean = FILTER id_tweets BY id_str IS NOT NULL;
id_tweets_clean = FILTER id_tweets_clean BY tweets IS NOT NULL;

-- group hashtags by user
group_id_tweets = GROUP id_tweets_clean BY id_str;

-- convert group:username{(username,hashtag),(username,hashtag)} into username hashtag,hashtag
set_id_tweets = FOREACH group_id_tweets GENERATE group, (CHARARRAY)CSV.ToCSV(id_tweets_clean.tweets) AS tweet_para;

--dump set_id_tweets;
-- save result in sequence file format
STORE set_id_tweets INTO 'from_prometheus/tweets_per_id' USING com.twitter.elephantbird.pig.store.SequenceFileStorage(
        '-c com.twitter.elephantbird.pig.util.TextConverter',
        '-c com.twitter.elephantbird.pig.util.TextConverter'
);
