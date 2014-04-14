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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
public class FrequentItem {

	public static void main(String[] args) throws Exception,ArrayIndexOutOfBoundsException {
		String frequentPatternsFilename = "";
		double minSupport = 0.0;
		int minSetLength = 1;
		long numOfTransactions = 1;
		String sortBy = ""; 
		try{ 	
			if (args.length < 6 || args.length > 10) {
				System.err.println("hadoop jar fItemSet -f <filename> -s <minSupport> -l <minSetLength> -n <numOfTransactions> -r <sortBy>");
				return;
			}else{
				for(int i=0;i <= (args.length-1);i++){
					switch(args[i]){
					case "-f":frequentPatternsFilename = args[i+1];break;
					case "-s":minSupport = Double.parseDouble(args[i+1]);break;
					case "-l":minSetLength = Integer.parseInt(args[i+1]);break;
					case "-n":numOfTransactions = Long.parseLong(args[i+1]);break;
					case "-r":sortBy = args[i+1].toLowerCase();break;
					}
				}
				if (minSupport > 1 && minSupport <= 100) minSupport = minSupport/100.00;
				if(frequentPatternsFilename.length() > 0 && 
						(sortBy.equals("") || sortBy.equals("l") || sortBy.equals("s"))&&
						minSetLength >= 1 && 
						(minSupport >= 0.0 && minSupport <= 1.0)
						){
					Configuration conf = new Configuration();
					readFrequentPatterns(conf,frequentPatternsFilename,minSupport,minSetLength,numOfTransactions,sortBy);
				}
				else{
					System.err.println("fItemSet -f filename -s <minSupport> -l <minSetLength<minim is 1>> -n <numOfTransactions>-r <sortBy>");
					return;
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void readFrequentPatterns(Configuration conf,String frequentpatterns,
			double minSupport,int length,long numOfTransactions,String sortBy) throws IOException{

		Map<List,Double> unsortedMap = new HashMap<List,Double>();		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] filestatus = fs.listStatus(new Path(frequentpatterns));

		for (FileStatus item : filestatus) {
			if(item.getPath().getName().startsWith("part")){
				Reader frequentPatternsReader = new SequenceFile.Reader(fs,new Path(frequentpatterns + item.getPath().getName()), conf);
				Text key = new Text();
				TopKStringPatterns value = new TopKStringPatterns();
				while(frequentPatternsReader.next(key,value)){
					List<Pair<List<String>,Long>> patterns = value.getPatterns();
					for(Pair<List<String>, Long> pair: patterns) {
						List<String> itemList = pair.getFirst();
						Long occurrence = pair.getSecond();
						double support = (double) occurrence / numOfTransactions;
						if(itemList.size() >= length && support >= minSupport){
							unsortedMap.put(itemList, support);
						}
					}
				}
				frequentPatternsReader.close();
			}
		}
		printMap(unsortedMap,sortBy);   
	}
	
	private static Map sortByComparator(Map unsortedMap) { 
		List list = new LinkedList(unsortedMap.entrySet());
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
	
	public static void printMap(Map<List, Double> unsortedMap, String sortBy) {
		try{
			File file = new File("frequentitemsets.txt");
			if(!file.exists()) file.createNewFile();
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			if(sortBy.equalsIgnoreCase("s")){
				Map<List,Double> mapSup = sortByComparator(unsortedMap);
				for (Map.Entry entry : mapSup.entrySet()) {
					bw.write(entry.getKey() + " sup: " + entry.getValue() + "\n");	
				}
			}else if(sortBy.equalsIgnoreCase("l")){
				Map<List,Integer> mapSize = new HashMap<List, Integer>();
				for(Map.Entry entry: unsortedMap.entrySet()){
					List<String> hashtags = (List<String>) entry.getKey();
					mapSize.put(hashtags, hashtags.size());
				}
				Map<String,Double> mapSetSize = sortByComparator(mapSize);
				for (Map.Entry setSize : mapSetSize.entrySet()) {
					for(Map.Entry sup : unsortedMap.entrySet()){
						if(setSize.getKey().equals(sup.getKey())){
							bw.write(setSize.getKey() + " sup: " + sup.getValue() + "\n");
						}
					}
				}
			}else{
				for(Map.Entry entry : unsortedMap.entrySet()){
					bw.write(entry.getKey() + " sup: " + entry.getValue() + "\n");
				}
			}
			bw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
