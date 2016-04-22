//imports
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

class mySortClass extends Thread{ //Class that implements sort parallelly based on the number of threads.
	String tempDir;
	int filenum; 
	List<String> data;
	public mySortClass(String tempDir, int fnum, List<String> unsortedData){
		this.tempDir = tempDir;
		filenum = fnum;
		data = unsortedData;
	}
	public void run(){
		if(data.size() != 0){
		quickSort(0,data.size()-1);
		try{
			FileWriter fw = new FileWriter(tempDir+"/temp-" +filenum+".txt");
			for(int i=0;i<data.size();i++)
			{
				fw.write(data.get(i)+"\n");
				fw.flush();
			}
			fw.close();
		}catch(IOException ioe){
			System.out.println("Error from Run Method: "+ioe.getMessage());
		}
		}
	}
	
	//REFERENCE: 
	//http://javarevisited.blogspot.com/2014/08/quicksort-sorting-algorithm-in-java-in-place-example.html
	//Does an in-place sorting of the data that it gets.
	private void quickSort(int low, int high) {
		int i = low, j = high;
	    // Get the pivot element from the middle of the list
	    String pivot = data.get(low + (high-low)/2).substring(0, 10);

	    // Divide into two lists
	    while (i <= j) {
	    	// If the current string from the left list is smaller then the pivot
	    	// string then get the next string from the left list
	    	while (data.get(i).substring(0, 10).compareTo(pivot) < 0) {
	    		i++;
	    	}
	    	// If the current value from the right list is larger then the pivot
	    	// element then get the next element from the right list
	    	while (data.get(j).substring(0, 10).compareTo(pivot) > 0) {
	    		j--;
	    	}
	    	if (i <= j) {
	    		swap(i, j);
	    		i++;
	    		j--;
	        }
	    }
	    // Recursion
	    if (low < j)
	    	quickSort(low, j);
	    if (i < high)
	    	quickSort(i, high);
	}
	private void swap(int i, int j){
		String temp = data.get(i);
		data.set(i,data.get(j));
		data.set(j, temp);
	}
}

public class sortFile_multithreaded {
	private static int filenum = 0;
	public synchronized static void increment(){
        filenum++;
    }

    public synchronized static void decrement(){
        filenum--;
    }

    public  static int value(){
        return filenum;
    }
	
	//This function reads the raw input file and converts small files which are created using mySortClass.
	//mysortClass takes the unsorted data and sorts it and stores in an intermediate temporary file.
	public static int readAndPartition(String sourceFile, String tempDir,int threadnum) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(sourceFile));
		String str = br.readLine();	
		//int filenum = 0;
		
		ArrayList<String> input = new ArrayList<String>();
		input.ensureCapacity(20000);
		while( str != null ) {
			input.add(str);	
			
			if(input.size() == 20000) {
				Thread t[] = new Thread[threadnum];
				int range =(int)Math.ceil((double)input.size()/threadnum);
				
				for(int i=0,start = 0;i<threadnum;i++){
					List<String> temp = new ArrayList<String>();
					for(int j=start;j<start+range;j++){
						if(j >= input.size())
							break;
						else
							temp.add(input.get(j));
						start+=range;
					}
					//System.out.println(temp.size()+"\n");
					t[i] = new mySortClass(tempDir, value(), temp);
					t[i].start();
					//filenum++;
					
				}
				increment();
				for(int i=0;i<threadnum;i++){
					t[i].join();
				}
				input = new ArrayList<String>();
			}
			str = br.readLine();
		}
		br.close();
		
		//if the arraylist (last block) is less than 20000 lines
		if(input.size() !=0 && input.size()<20000){
			Thread t[] = new Thread[threadnum];
			int range =(int)Math.ceil((double)input.size()/threadnum);
			for(int i=0,start = 0;i<threadnum;i++)
			{
				List<String> temp = new ArrayList<String>();
				for(i=start;i<start+range;i++){
					if(i == input.size())
						break;
					temp.add(input.get(i));
				}
				t[i] = new mySortClass(tempDir, value(), temp);
				t[i].start();
				//filenum++;
				start+=range;
			}
			increment();
			for(int i=0;i<threadnum;i++){				
				t[i].join();
			}
		}
		int totalfiles = value();
		return totalfiles;
	}
	//This function merges all the intermediate files created in temp file and creates a single outputFile
	public static void mergeTempFiles(int totalfile,String tempDir, String outputFile)throws IOException{
		
		Map<String, String> map = new TreeMap<String, String>(); 
		
		BufferedReader[] br = new BufferedReader[totalfile];
		for(int i=0; i<totalfile; i++) {
			try{
				br[i] = new BufferedReader(new FileReader(tempDir+"/temp-" +i+".txt"));
				String str = br[i].readLine();
				if(str != null){
					String key = str.substring(0,10);
					String val = i+"$$"+str.substring(10);
					map.put(key, val);
				}
			}catch(FileNotFoundException ffe){}
		}

		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));		
		
		while(!map.isEmpty()) {
			String str = map.keySet().iterator().next();
			String val = map.get(str);
			StringTokenizer st =new StringTokenizer(map.get(str), "$$");
			int filenum = Integer.parseInt(st.nextToken());
			map.remove(str);
			bw.write(str+st.nextToken());
			bw.write("\n");	
			bw.flush();
			str = br[filenum].readLine();
			if(str != null ) {				
				String key = str.substring(0,10);
				val = filenum+"$$"+str.substring(10);
				map.put(key, val);
			}
		}
		bw.close();
		//Deleting all the temporary files and closing all the buffer readers.
		for(int i=0; i<br.length; i++) {
			try{
				if(br[i] != null)
					br[i].close();
				new File(tempDir+"/temp-" +i+".txt").delete();
			}catch(FileNotFoundException ffe){}
		}
	}
	
	public static void main(String[] args)throws Exception{
		String sourceFile = "/mnt/raid/input"; // Input file name
		String tempDirLoc = "/mnt/raid/tmp"; //Temporary file creation directory
		String outputFile = "/mnt/raid/output"; // Output file name
		int threadnum = Integer.parseInt(args[0]);
		
		long startTime = System.currentTimeMillis();
		
		int fileNum = readAndPartition(sourceFile, tempDirLoc,threadnum);
		mergeTempFiles(fileNum, tempDirLoc, outputFile);
		
		long endTime = System.currentTimeMillis();
		System.out.println(threadnum +"Threads Time taken = " +(double)(endTime-startTime)/1000 + " seconds");
	}
}
