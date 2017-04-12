package MergeSort;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;


import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;


public class ZK_MergeSort implements Callable<String>  {

	private int[] values;
	public Thread t;
	
	private static Logger LOG = Logger.getLogger(ZK_MergeSort.class);
	
	private static String LEADER_ELECTION_ROOT_NODE = "/election";
	private static String PROCESS_NODE_PREFIX = "/p_";
	
	private int id;
	private ZooKeeperService zooKeeperService;
	
	private String processNodePath;
	private String watchedNodePath;
	private String name;
	private volatile boolean leader=false; 
	private volatile boolean removed=false;
	private PCQ tasks=null;
	private PCQ results=null;
	
	
	public ZK_MergeSort(String name, int[] values, int id, String zkURL, PCQ tasks, PCQ results) throws IOException  {
		
		//QueueBuilder<?>    builder = QueueBuilder.builder(client, consumer, serializer, path);
		this.tasks=tasks;
		this.results=results;
		this.name=name;
		this.values = values;
		this.id=id;
		zooKeeperService = new ZooKeeperService(zkURL, new ProcessNodeWatcher());

		//Callable code here
		if(LOG.isInfoEnabled()) {
			LOG.info("Process with id: " + id + " has started!");
		}
		
		final String rootNodePath = zooKeeperService.createNode(LEADER_ELECTION_ROOT_NODE, false, false);
		if(rootNodePath == null) {
			throw new IllegalStateException("Unable to create/access leader election root node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		
		processNodePath = zooKeeperService.createNode(rootNodePath + PROCESS_NODE_PREFIX, false, true);
		if(processNodePath == null) {
			throw new IllegalStateException("Unable to create/access process node with path: " + LEADER_ELECTION_ROOT_NODE);
		}
		
		if(LOG.isDebugEnabled()) {
			LOG.debug("[Process: " + id + "] Process node created with path: " + processNodePath);
		}

		attemptForLeaderPosition();
		
	}

	public String call() throws Exception{
	
		if (leader) {
			System.out.println("leader start");
			
			
			ArrayList<int[]> parts = split(3);
			for (int[] a : parts) {
				tasks.produce(a);
			}

			ArrayList<int[]> resultParts = new ArrayList<int[]>();
			
		
			while (results.getCount()!=3) {
			}
				
			//mod
			final List<String> childNodePaths = zooKeeperService.getChildren(LEADER_ELECTION_ROOT_NODE, false);
			if(!childNodePaths.contains(processNodePath)){
				System.out.println(processNodePath+" no longer exit");
				return "removed";
			}
			
			
			resultParts= results.consumeAll();							
			int[] finalresult =finalMerge(resultParts);
			System.out.println(name+" report final result: "+Arrays.toString(finalresult));		
			return "done";
			
		} else {
						
			System.out.println("follower start");		
			int[] info = tasks.consume();
			mergeSort(info);
			Thread.sleep(new Random().nextInt(5000));
			results.produce(info);
			System.out.println(Arrays.toString(info)+" "+"from "+name);
			
			
			while(leader==false){
				
			}
					
			ArrayList<int[]> resultParts = new ArrayList<int[]>();
			while (results.getCount() != 3) {

			}
			resultParts = results.consumeAll();

			int[] finalresult = finalMerge(resultParts);
			System.out.println(name + " report final result: " + Arrays.toString(finalresult));
			return "done";
		
			
		
		}

		//return "done";
	}
	
	public class ProcessNodeWatcher implements Watcher{
		public void process(WatchedEvent event) {
			if(LOG.isDebugEnabled()) {
				LOG.debug("[Process: " + id + "] Event received: " + event);
			}
			
			final EventType eventType = event.getType();
			if(EventType.NodeDeleted.equals(eventType)) {
				if(event.getPath().equalsIgnoreCase(watchedNodePath)) {
					attemptForLeaderPosition();
				}
				
			}		
		}	
		
		
	}
	
	//modified boolean leader
	private void attemptForLeaderPosition() {
		
		final List<String> childNodePaths = zooKeeperService.getChildren(LEADER_ELECTION_ROOT_NODE, false);
			
		Collections.sort(childNodePaths);
		
		int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
	
		//execute something
		if(index == 0) {
			if(LOG.isInfoEnabled()) {
				LOG.info("[Process: " + id + "] I am the new leader!");
				leader=true;
			}
		} else {
			final String watchedNodeShortPath = childNodePaths.get(index - 1);
			
			watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;
			
			if(LOG.isInfoEnabled()) {
				LOG.info("[Process: " + id + "] - Setting watch on node with path: " + watchedNodePath);
			}
			zooKeeperService.watchNode(watchedNodePath, true);
		}
	}
	
	
	public void mergeSort(int[] a) {
		if (a.length <= 1)
			return;

		int mid = a.length / 2;

		int[] left = Arrays.copyOfRange(a, 0, mid);
		int[] right = Arrays.copyOfRange(a, mid, a.length);

		mergeSort(left);
		mergeSort(right);

		merge(left, right, a);
	}

	public void merge(int[] a, int[] b, int[] r) {
		
		int i = 0, j = 0, k = 0;
		
		while (i < a.length && j < b.length) {
			if (a[i] < b[j])
				r[k++] = a[i++];
			else
				r[k++] = b[j++];
		}

		while (i < a.length)
			r[k++] = a[i++];

		while (j < b.length)
			r[k++] = b[j++];
	}


	public  ArrayList<int[]> split(int numPart){
		ArrayList<int[]> parts = new ArrayList<int[]>();
		ArrayList<ArrayList<Integer>> aoa = new ArrayList<ArrayList<Integer>>();
		
		for(int i=0;i<numPart;i++){
			aoa.add(new ArrayList<Integer>());
		}
			
		for(int i=0;i<values.length;i++){
			aoa.get(i%numPart).add(values[i]);				
		}
				
		for(ArrayList<Integer> a:aoa){
			int[] ints = new int[a.size()];
			for(int i=0, len = a.size(); i < len; i++)
			   ints[i] = a.get(i);
			parts.add(ints);
		}
		
		return parts;
	}
	
	
	public int[] finalMerge(ArrayList<int[]> parts) 
	{
		int[] a = new int[0];

		int[] answer=null;

		for (int[] b : parts) 
		{		
			answer = new int[a.length + b.length];
			int i = 0, j = 0, k = 0;
			while (i < a.length && j < b.length) {
				if (a[i] < b[j]) {
					answer[k] = a[i];
					i++;
				} else {
					answer[k] = b[j];
					j++;
				}
				k++;
			}

			while (i < a.length) {
				answer[k] = a[i];
				i++;
				k++;
			}

			while (j < b.length) {
				answer[k] = b[j];
				j++;
				k++;
			}
			
			a=answer;
		}
		
		return answer;
	}

	
	
}
