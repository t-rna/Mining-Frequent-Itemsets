import java.io.*;
import java.nio.file.*;
import java.util.*;

public class APriori {
	
	private int maxBaskets = 0;					// Before reading our data set, there are 0 baskets.
	private int basketCounter = 0;				// Which basket in the file
	private boolean firstExec = true;			// If first execution of algorithm, need to count baskets.
	private int support = 0;
	private int chuck = Integer.MAX_VALUE;		/* Since we don't know how many baskets there are initially, 
												    we set the chuck to an arbitrarily large number. 
												    
												   Note: Here, a chuck is an integer value representing the
												         fractional amount of the entire data size (maxBaskets)
												         Eg. We want 10% of 8816 then chuck = 8816 
												 */
	
	// Count the occurrence of each item (known as singletons)
	private Map<Integer, Integer> singletons = new HashMap<>();
	
	// Frequent singletons are those items with a count >= support 
	private Vector<Integer> freqItems = new Vector<>();
	
	// Frequent pairs and their counts are stored using triples implemented by this data structure
	private Map<Integer, HashMap<Integer, Integer>> freqPairs = new HashMap<>();
	
	/**
	 * This method conducts the first pass over the dataset indicated by
	 * the filepath parameter. It will read each basket and count the 
	 * occurrence of each individual item and maintain the count in the
	 * singletons HashMap. During the first pass is when the number of 
	 * baskets in the dataset is determined.
	 * 
	 * @param fp	The filepath of the dataset.
	 * @throws IOException
	 */
	private void firstPass(String fp) throws IOException {
		Path file = Paths.get(fp);
		
		try (InputStream in = Files.newInputStream(file);
			 BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
			
		    String line = null;
		    String[] tokens;
		    
		    while ((line = reader.readLine()) != null && basketCounter <= chuck) {
		        tokens = line.split(" ");	        
		        for (int i = 0; i < tokens.length; i++) {
		        	int item = Integer.parseInt(tokens[i]);
		        	
		        	if (singletons.containsKey(item))
		        		singletons.put(item, singletons.get(item) + 1);
		        	else
		        		singletons.put(item, 1);
		        }
		        
		        basketCounter++;	        
		    }
		} catch (IOException x) {
		    System.err.println(x);
		}
		
		if (firstExec)
			maxBaskets = basketCounter;
		/* FOR TESTING
		for (Map.Entry<Integer, Integer> ic : singletons.entrySet())
			System.out.println("index: " + ic.getKey() + ", count: " + ic.getValue());
		System.out.println(singletons.size() + " ...end of pass 1...");
		*/
	}
	
	/**
	 * This method is the intermediate step between the first and second pass
	 * of the A Priori algorithm. Generates a list (implemented as a vector)
	 * of frequent items (ie. item count >= support) 
	 */
	private void makeFreqItemsList() {
		Integer itemCount;	
		for (Map.Entry<Integer, Integer> entry : singletons.entrySet()) {
			itemCount = entry.getValue();
			if (itemCount >= support) {
				//System.out.println(itemCount + " >= " + support);
				freqItems.add(entry.getKey());
			}
		}
		
		/* FOR TESTING
		for (int i = 0; i < freqItems.size(); i++) 
			System.out.println("index: " + i + ", item: " + freqItems.get(i));
		System.out.println("...end of pruning...");
		*/
	}
	
	/**
	 * This method conducts the second pass over the dataset indicated by
	 * the filepath parameter. It will read each basket and generate pairs
	 * using a nested for loop. Only pairs with elements that are frequent
	 * (ie. exist in the frequent item's list) are recorded as candidate 
	 * pairs using the triples approach, [item1, item2, count]. Repeated
	 * pairs causes their triples' count to be incremented.
	 * 
	 * @param fp	The filepath of the dataset.
	 * @throws IOException
	 */
	private void secondPass(String fp) throws IOException {
		Path file = Paths.get(fp);
		
		try (InputStream in = Files.newInputStream(file);
			 BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
			
		    String line = null;
		    String[] tokens;
		    
		    while ((line = reader.readLine()) != null && basketCounter <= chuck) {
		        tokens = line.split(" ");
		        
		        for (int i = 0; i < tokens.length; i++) {		        	
		        	for (int j = i+1; j < tokens.length; j++) {
		        		int p1 = Integer.parseInt(tokens[i]);
		        		int p2 = Integer.parseInt(tokens[j]);
		        		
		        		// Check if each element of the pair is frequent
		        		if (freqItems.contains(p1) && freqItems.contains(p2)) {
		        			
		        			// If the pair exists, increment the count...
		        			if (freqPairs.containsKey(p1)) {
		        				if (freqPairs.get(p1).containsKey(p2)) {
		        					Integer pairCount = freqPairs.get(p1).get(p2);
		        					freqPairs.get(p1).put(p2, pairCount + 1);
		        				} else {
		        					freqPairs.get(p1).put(p2, 1);
		        				}		        					
		        			} 
		        			// Otherwise, create a new triple with a count of 1.
		        			else
		        				freqPairs.put(p1, new HashMap<Integer, Integer>(){{put(p2, 1);}});
		        		}    	
		        	}
		        }
		        basketCounter++;
		    }
		} catch (IOException x) {
		    System.err.println(x);
		}
		
		/* FOR TESTING
		for (Map.Entry<Integer, HashMap<Integer, Integer>> first : freqPairs.entrySet()) {
		    Integer i = first.getKey();
		    for (Map.Entry<Integer, Integer> second : first.getValue().entrySet()) {
		        Integer j = second.getKey();
		        Integer c = second.getValue();
		        
		        System.out.println("[" + i + ", " + j + ", " + c +"]");
		    }
		}
		System.out.println("...end of pass 2...");
		*/
	}
	
	/**
	 * Method sets the chuck of the dataset to be considered, as an integer,
	 * based on a percentage of the overall dataset size. This method is 
	 * intended to be used before the first pass of the next cycle of
	 * algorithm execution.
	 * 
	 * @param percentCh		Percentage, as a decimal, of the dataset to use.
	 */
	private void configNextRound(int percentCh) {
		chuck = (int)((double)percentCh/100 * maxBaskets);	// chuck is percentage of our dataset, converted into an integer number of baskets
		basketCounter = 0;									// reset counter to 0 for first pass of next cycle
	}
	
	/**
	 * Method intended to be used immediately after the first pass, where 
	 * the number of baskets was determined. Sets the support threshold,
	 * as an integer, based on a percentage of the overall dataset size.
	 * 
	 * @param percentSupp	Percentage, as a decimal, of the support threshold.
	 */
	private void configSupp(double percentSupp) {
		if (firstExec)
			support = (int)(percentSupp * maxBaskets);		/* After the first pass of the first run, 
															   support is a fraction of the maximum dataset */
		else
			support = (int)(percentSupp * chuck);			/* In subsequent runs after the first, we're accessing
															   only a fraction of the maximum dataset (ie. chuck value) */
		
		basketCounter = 0;		// set counter back to zero for second pass
	}
	
	/**
	 * This method executes the logic for the scalability study outlined in
	 * 60-475 Project 1. The user provides a support threshold, as a
	 * percentage, and the method executes a benchmark of the algorithm
	 * on dataset sizes of 1%, 5%, 10%, 20%, ... , 100% of the dataset.
	 * 
	 * @param supp		The support, as a decimal, for the study.
	 */
	public void runStudy(double supp) {
		String fp = System.getProperty("user.dir") + "\\src\\retail.txt";	/* This filepath relates to where Eclipse instantiates
		   																	   the JVM (ie. the root project directory) */
		int dsize = 100;
		
		for (int i = 0; i < 12; i++) {
			
			// Start from 100% dataset size down to 1% in 10% increments
			if (i > 0 && i < 10) {
				firstExec = false;	// after first execution (i=0), dataset size is known (maxBaskets)
				configNextRound( (dsize -= 10) );
			}
			if (i == 10) {
				configNextRound( (dsize -= 5) );
			}
			if (i == 11) {
				configNextRound( (dsize -= 4) );
			}
			
			// Only interested in execution time of the algorithmic code.
			long startTime = System.currentTimeMillis();		
			try {
				firstPass(fp);
				configSupp(supp);		// set support immediately after first pass 
				makeFreqItemsList();
				secondPass(fp);
			} catch (Exception e) { System.out.println(e); }
			
			long endTime = System.currentTimeMillis();
			long runTime = endTime - startTime;
			System.out.println("(" + dsize + "% of Data Size: " + maxBaskets + ")");
			System.out.println("  Runtime: " + runTime + " ms");
			System.out.println("  Baskets: " + chuck + ", Support: " + support + "\n");
			
			// Clear out the previous run's tables
			singletons = null;	singletons = new HashMap<>();
			freqItems = null;	freqItems = new Vector<>();
			freqPairs = null;	freqPairs = new HashMap<>();
		}
	}
	
	public static void main(String[] args) {
		APriori ap = new APriori();
		ap.runStudy(0.10);				// specify a support percent
	}
}
