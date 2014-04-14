--Load data
REGISTER '/share/lib/elephant-bird-pig-4.1.jar';
REGISTER '/share/lib/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '/share/lib/google-collections-1.0.jar';
REGISTER '/share/lib/json-simple-1.1.jar';
REGISTER '/usr/lib/hadoop/lib/hadoop-lzo-cdh4-0.4.15-gplextras.jar';
REGISTER '/opt/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/pig/pig-0.11.0-cdh4.4.0.jar';

REGISTER 'ToSet.py' USING org.apache.pig.scripting.jython.JythonScriptEngine AS csv;
--Load twitter dataset
raw_data = LOAD 'twitter_dataset' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);
--We need hashtags and usernames
fields = FOREACH raw_data GENERATE FLATTEN(json#'entities'#'hashtags'),json#'user'#'name'as username;
hashtags = FOREACH fields GENERATE FLATTEN($0#'text') AS hashtag,username;
--Remove with empty hashtag
hashtags = FILTER hashtags BY hashtag IS NOT NULL;
--group hashtags by username
grp_hashtags = GROUP hashtags BY username;
--grp_hashtags is in this format: bag{(group,hashtag)}
-- we need to convert it into username \t hashtag1 hashtag2 hashtag3
convert = foreach grp_hashtags generate group as username, csv.ToCSV(hashtags.hashtag);
--Store into output directory
store convert into 'output' using PigStorage('\t')
