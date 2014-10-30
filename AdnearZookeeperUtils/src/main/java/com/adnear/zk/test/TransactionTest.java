package main.java.com.adnear.zk.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class TransactionTest {
	
	private static final int TICKTIME = 1;
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {  
		if (args.length < 4) {
			System.out.printf("Usage: %s <zk-connection-string> <parent-znode> <child-znode-1> <child-znode-2> "
					+ "[<child-znode-n> ...]\n",
			TransactionTest.class.getSimpleName());
			System.exit(1);
		}
		
		ConnectionTest conn = new ConnectionTest();
		ZooKeeper zk = conn.connect(args[0], TICKTIME);
		
		String topZnodePath = "/txn-test";
		if(zk.exists(topZnodePath, false) == null) {
			System.out.printf("Creating top level znode %s for transaction test\n", topZnodePath);
			zk.create(topZnodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		
		String baseParentPath = topZnodePath + "/" + args[1] + "-";
		String parentPath = zk.create(baseParentPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.printf("Created parent znode %s\n", parentPath);
		
		List<String> childPaths = new ArrayList<String>(args.length - 2);
		Transaction txn = zk.transaction();
		for (int i = 2; i < args.length; i++) {
			String childPath = parentPath + "/" + args[i];
			childPaths.add(childPath);
			System.out.printf("Adding create op with child path %s\n", childPath);
			txn.create(childPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		System.out.println("Committing transaction");
		List<OpResult> opResults = txn.commit();
		
		System.out.println("Transactions results:");
		for (int i = 0; i < opResults.size(); i++) {
			OpResult opResult = opResults.get(i);
			int type = opResult.getType();
			String childPath = childPaths.get(i);
			switch (type) {
				case ZooDefs.OpCode.create:
					System.out.printf("Child node %s created successfully\n", childPath);
					break;
				case ZooDefs.OpCode.error:
					System.out.printf("Child node %s was not created. There was an error.\n", childPath);
					break;
				default:
					System.out.printf("Don't know what happened with child node %s! OpResult type: %d", childPath, type);
			}
		}
	}

}
