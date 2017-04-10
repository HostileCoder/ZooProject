package MergeSort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

public class ZK_Main {
	
	static Logger LOG = Logger.getLogger(ZK_Main.class);
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		
		ArrayList<Future<String>> futures=new ArrayList<Future<String>> ();	
		ArrayList<Callable<String>> processes=new ArrayList<Callable<String>> ();	
		PCQ  tasksQ = new PCQ("localhost:2181", "/app1");
		PCQ resultsQ = new PCQ("localhost:2181", "/app2");
		
		int[] input = {100, 33, 111, 63, 67, 886};
		int numServer=4;

		for(int i=0;i<numServer;i++){
			processes.add(new ZK_MergeSort(Integer.toString(i), input, i, "localhost:2181",tasksQ, resultsQ));
		}
		
		
		ExecutorService service = Executors.newFixedThreadPool(4);
		for(Callable<String> c:processes){
			Future<String> f= service.submit(c);
			futures.add(f);
			//f.get();
		}
		
		//service.submit(processes.get(2));
		
		System.out.println(futures.get(0).get());
	
	
	}
		
}
