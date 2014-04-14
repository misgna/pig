import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Seconds;


public class AssociationRule {
	private final static Logger log = Logger.getLogger(AssociationRule.class);
	public static void main(String[] args) throws Exception,ArrayIndexOutOfBoundsException {
		String patterns = "",frequentpatterns = "", fList = "";
		double minSupport = 0.0,minConfidence = 0.0;
		long numOfTweets = (long)1;
		String sortBy = ""; 
		DateTime startDate = new DateTime();
		try{ 	
			if (args.length < 6 || args.length > 12) {
				System.err.println("hadoop jar asRule.jar -f <patterns directory path> -s <minSupport> -c <minConfidence> -n <numOfTrweets> -r <sortBy>");
				return;
			}else{
				for(int i=0;i <= (args.length-1);i++){
					switch(args[i]){
					case "-f":patterns = args[i+1];break;
					case "-s":minSupport = Double.parseDouble(args[i+1]);break;
					case "-c":minConfidence = Double.parseDouble(args[i+1]);break;
					case "-n":numOfTweets = Long.parseLong(args[i+1]);break;
					case "-r":sortBy = args[i+1].toLowerCase();break;
					}
				}
				if (minSupport > 1 && minSupport <= 100) minSupport = minSupport/100.00;
				if(patterns.length() > 0 && 
						(sortBy.equals("")  || sortBy.equals("s") || sortBy.equals("c")) && 
						(minSupport >= 0.0 && minSupport <= 1.0)
						){
					frequentpatterns = patterns + "/frequentpatterns/";
					fList = patterns + "/fList";
					Configuration conf = new Configuration();
					Map<List<String>,Long> frequency = getFrequency(frequentpatterns,fList,minSupport,numOfTweets);
					readFrequentPatterns(conf,frequentpatterns,frequency,minSupport,minConfidence,numOfTweets,sortBy);
				}
				else{
					System.err.println("hadoop jar asRule.jar -f <filename> -s <minSupport> -n <numOfTweets> -r <sortBy>");
					return;
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}


		DateTime endDate = new DateTime();
		log.info("Takes " + Days.daysBetween(startDate, endDate).getDays() + " days, "
				+ Hours.hoursBetween(startDate, endDate).getHours() % 24 + " hours, "
				+ Minutes.minutesBetween(startDate, endDate).getMinutes() % 60 + " minutes, "
				+ Seconds.secondsBetween(startDate, endDate).getSeconds() % 60 + " seconds.");
	}
	public static void readFrequentPatterns(Configuration conf,String frequentpatterns,Map<List<String>,Long> frequency,
			double minSupport,double minConfidence,long numOfTweets,String sortBy) throws IOException{

		Map<String,Double> supMap = new HashMap<String,Double>();
		Map<String,Double> conMap = new HashMap<String,Double>();

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] filestatus = fs.listStatus(new Path(frequentpatterns));

		log.info("Creating association rule :");

		/*
		 * Read all files starts with "part" from frequentpatterns directory that 
		 * are created by Apache Mahout FPG.
		 */ 
		for (FileStatus item : filestatus) {

			if(item.getPath().getName().startsWith("part")){
				Reader frequentPatternsReader = new SequenceFile.Reader(fs,new Path(frequentpatterns + item.getPath().getName()), conf);
				Text key = new Text();
				TopKStringPatterns value = new TopKStringPatterns();

				while(frequentPatternsReader.next(key, value)) {
					List<Pair<List<String>, Long>> patterns = value.getPatterns();
					for(Pair<List<String>, Long> pair: patterns) {
						List<String> hashtagList = pair.getFirst();
						Long occurrence = pair.getSecond();

						/*
						  Do not create powerset of hashtag list if the support of the list
						  is less than the minimum support and if it has a single element. */
						double supportOfSet = (double)occurrence/(double)numOfTweets;
						if(supportOfSet >= minSupport && hashtagList.size() > 1){
							List<List<String>> possibleSets = powerset(hashtagList);
							for(List<String> antecedent : possibleSets){
								if(!antecedent.isEmpty()){									
									for(List<String> consequent:possibleSets){										
										if(!consequent.isEmpty() && !antecedent.equals(consequent)){											
											consequent.remove(antecedent);
											List<String> onlyInConsequent = new LinkedList<String>(consequent);
											onlyInConsequent.removeAll(antecedent);
											if(!onlyInConsequent.isEmpty()) {
												List<String> hashtagsUnion = new ArrayList<String>();
												hashtagsUnion.addAll(antecedent);
												hashtagsUnion.addAll(onlyInConsequent);
												Collections.sort(hashtagsUnion);
												Collections.sort(antecedent);
												Collections.sort(onlyInConsequent);
												if(frequencyOf(hashtagsUnion,frequency)!= 0 && frequencyOf(antecedent,frequency) != 0){
													double allFrequency = frequencyOf(hashtagsUnion,frequency);
													double support = allFrequency/numOfTweets;
													double confidence = (double)allFrequency / frequencyOf(antecedent,frequency); 
													if(confidence >= minConfidence){
														String rule = antecedent + "-->" + onlyInConsequent;
														supMap.put(rule, support);
														conMap.put(rule, confidence);

													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				frequentPatternsReader.close();
			}
		}
		printMap(supMap, conMap, sortBy);
	}
	public static long frequencyOf(List<String> key,Map<List<String>,Long> frequency){
		long occurrence = (long) 0;
		List<String> hashtagList = new LinkedList<String>();
		if(frequency.get(key) != null){
			occurrence = frequency.get(key);
		}
		else{
			Iterator<Map.Entry<List<String>, Long>> entries = frequency.entrySet().iterator();
			while (entries.hasNext()) {
				Map.Entry<List<String>, Long> entry = entries.next();
				if(entry.getKey().containsAll(key)){
					if(entry.getValue() > occurrence){
						occurrence = entry.getValue();
						hashtagList = entry.getKey();
					}
				}
			}
			if(occurrence != 0){
				List<List<String>> possibleSets = powerset(hashtagList);
				for(List<String> lists : possibleSets){
					if(!lists.isEmpty() && (frequency.get(lists) == null)){
						frequency.put(lists, occurrence);
					}
				}
			}
		}

		return occurrence;
	}
	public static List<List<String>> powerset(Collection<String> list) {
		List<List<String>> ps = new ArrayList<List<String>>();
		ps.add(new ArrayList<String>());   // add the empty set

		// for every item in the original list
		for (String item : list) {
			List<List<String>> newPs = new ArrayList<List<String>>();

			for (List<String> subset : ps) {
				// copy all of the current powerset's subsets
				newPs.add(subset);

				// plus the subsets appended with the current item
				List<String> newSubset = new ArrayList<String>(subset);
				newSubset.add(item);
				newPs.add(newSubset);
			}

			// powerset is now powerset of list.subList(0, list.indexOf(item)+1)
			ps = newPs;
		}
		return ps;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Map sortByComparator(Map unsortMap) {
		List list = new LinkedList(unsortMap.entrySet());
		Collections.sort(list, new Comparator(){
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
			}
		});
		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	public static Map<List<String>,Long> getFrequency(String frequentpatterns,String fList,double minSupport,long numOfTweets) throws IOException{
		log.info("Start creating Map <hashtaglist,frequency>");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] filestatus = fs.listStatus(new Path(frequentpatterns));
		Map<List<String>,Long> frequencyMap = new HashMap<List<String>,Long>();
		for (FileStatus item : filestatus) {
			if(item.getPath().getName().startsWith("part")){
				Reader frequentPatternsReader = new SequenceFile.Reader(fs,new Path(frequentpatterns + item.getPath().getName()), conf);
				Text key = new Text();
				TopKStringPatterns value = new TopKStringPatterns();
				while(frequentPatternsReader.next(key, value)) {
					List<Pair<List<String>, Long>> patterns = value.getPatterns();
					for(Pair<List<String>, Long> pair: patterns) {
						List<String> hashtags = pair.getFirst();
						Long occurrence = pair.getSecond();

						Collections.sort(hashtags);
						frequencyMap.put(hashtags, occurrence);
					}
				}
				frequentPatternsReader.close();

				FileSystem fs1 = FileSystem.get(conf);
				Reader frequencyReader = new SequenceFile.Reader(fs1,new Path(fList), conf);
				Text key1 = new Text();
				LongWritable value1 = new LongWritable();
				while(frequencyReader.next(key1, value1)) {
					List<String> hashtagsFromFList = new ArrayList<String>();
					hashtagsFromFList.add(key1.toString());
					frequencyMap.put(hashtagsFromFList, value1.get());
				}
				frequencyReader.close();
			}
		}
		log.info("Map <hashtaglist,frequency> is created.");
		return frequencyMap;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void printMap(Map<String, Double> support, Map<String,Double> confidence, String sortBy) {

		if(sortBy.equalsIgnoreCase("s")){
			Map<String,Double> mapSup = sortByComparator(support);
			for (Map.Entry entry : mapSup.entrySet()) {
				for(Map.Entry input : confidence.entrySet()){
					if(entry.getKey().equals(input.getKey())){
						System.out.println(entry.getKey() + ", sup: " + entry.getValue() +", conf: " + input.getValue());
					}
				}
			}
		}else if(sortBy.equalsIgnoreCase("c")){
			Map<String,Double> mapCon = sortByComparator(confidence);
			for (Map.Entry input : mapCon.entrySet()) {
				for(Map.Entry entry : support.entrySet()){
					if(input.getKey().equals(entry.getKey())){
						System.out.println(entry.getKey() + ", sup: " + entry.getValue() +", conf: " + input.getValue());
					}
				}
			}
		}else{
			Map<String,Double> mapSup = sortByComparator(support);
			for (Map.Entry entry : mapSup.entrySet()) {
				for(Map.Entry input : confidence.entrySet()){
					if(entry.getKey().equals(input.getKey())){
						System.out.println(entry.getKey() + ", sup: " + entry.getValue() +", conf: " + input.getValue());
					}
				}
			}
		}
	}
}
