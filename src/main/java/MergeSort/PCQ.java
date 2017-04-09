package MergeSort;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


public class PCQ implements Watcher{
    static ZooKeeper zk = null;
    static Integer mutex;
    String root;
    
	public PCQ(String address, String name){
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
              
        this.root = name;
        // Create ZK node name
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                System.out
                        .println("Keeper exception when instantiating queue: "
                                + e.toString());
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception");
            }
        }
        
	}

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }

    
    boolean produce(int[] i) throws KeeperException, InterruptedException{
//        ByteBuffer b = ByteBuffer.allocate(4);
        byte[] value;

        // Add child with value i
//        b.putInt(i);
//        value = b.array();
        
        value=serialize(i);
        
        zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);

        return true;
    }
    
    int[] consume() throws KeeperException, InterruptedException{
        int[] retvalue = null;
        Stat stat = null;

        // Get the first element available
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.size() == 0) {
                    System.out.println("Going to wait");
                    mutex.wait();
                } else {
                    Integer min = new Integer(list.get(0).substring(7));
                    for(String s : list){
                        Integer tempValue = new Integer(s.substring(7));
                        //System.out.println("Temporary value: " + tempValue);
                        if(tempValue < min) min = tempValue;
                    }
                                       
                    String nodeName=String.format("%010d",min);                  
                    System.out.println("Temporary value: " + root + "/element" + min);
                                                              
                    byte[] b = zk.getData(root + "/element" + nodeName,
                                false, stat);
                              
                    zk.delete(root + "/element" + nodeName, 0);
//                    ByteBuffer buffer = ByteBuffer.wrap(b);
//                    retvalue = buffer.getInt();
                   
                    retvalue= (int[]) deserialize(b);
                    
                    
                    return retvalue;
                }
            }
       }
    }
    
    ArrayList<int[]> consumeAll() throws KeeperException, InterruptedException{
    	ArrayList<int[]> parts= new ArrayList<int[]>();
    	List<String> list = zk.getChildren(root, true);
    	for(String s:list){
    		byte[] b = zk.getData(root + "/element" + s, false, null);
    		int[] a=(int[]) deserialize(b);
    		parts.add(a);
    	}
    	return parts;
    }
    
    
    
    public static byte[] serialize(Object obj)  {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = null;
		try {
			os = new ObjectOutputStream(out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    try {
			os.writeObject(obj);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return out.toByteArray();
	}
	
	
	
	public static Object deserialize(byte[] data)  {
	    ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = null;
		try {
			is = new ObjectInputStream(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Object o=null;
	    try {
			o = is.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return o;
	}
    
    
    public static void main(String args[]) throws KeeperException, InterruptedException {
    	PCQ q = new PCQ("localhost:2181", "/app1");

        System.out.println("Input: " + args[0]);


      	int[] x={34,2323,565,3434};
      	int[] y={45,23,67,232,57};
        System.out.println("Producer");
                    q.produce(x);
                    q.produce(y);

        System.out.println("Consumer");
               for(int i=0;i<2;i++){
                    int[] r = q.consume();
                    System.out.println("Item: " + r[0]);
 
               }

    }
    
}
