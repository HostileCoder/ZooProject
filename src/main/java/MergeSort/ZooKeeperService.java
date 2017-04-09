/**
 * 
 */
package MergeSort;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import MergeSort.ZK_MergeSort.ProcessNodeWatcher;



/**
 * @author Sain Technology Solutions
 *
 */
public class ZooKeeperService {
	
	private ZooKeeper zooKeeper;
	
	public ZooKeeperService(String url,  ProcessNodeWatcher processNodeWatcher) throws IOException {
		zooKeeper = new ZooKeeper(url, 3000, processNodeWatcher);
	}

	public String createNode( String node, boolean watch,  boolean ephimeral) {
		String createdNodePath = null;
		try {
			
			Stat nodeStat =  zooKeeper.exists(node, watch);
			
			if(nodeStat == null) {
				createdNodePath = zooKeeper.create(node, new byte[0], Ids.OPEN_ACL_UNSAFE,
						(ephimeral ? CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
			} else {
				createdNodePath = node;
			}

		} catch (KeeperException e) {
			throw new IllegalStateException(e);
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return createdNodePath;
	}
	
	public boolean watchNode(String node, boolean watch) {
		
		boolean watched = false;
		try {
			final Stat nodeStat = zooKeeper.exists(node, watch);

			if (nodeStat != null) {
				watched = true;
			}

		} catch (KeeperException e) {
			throw new IllegalStateException(e);
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}

		return watched;
	}
	
	public List<String> getChildren( String node, boolean watch) {
		
		List<String> childNodes = null;
		
		try {
			childNodes = zooKeeper.getChildren(node, watch);
			
		} catch (KeeperException e) {
			throw new IllegalStateException(e);
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
		
		return childNodes;
	}
	
}
