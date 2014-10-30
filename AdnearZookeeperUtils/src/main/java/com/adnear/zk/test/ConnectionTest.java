package main.java.com.adnear.zk.test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ConnectionTest {
	
	private static final int SESSION_TIMEOUT = 5000;
	
	public ZooKeeper connect(String hosts) throws IOException, InterruptedException { 
		final CountDownLatch signal = new CountDownLatch(1);
		ZooKeeper zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
			public void process(WatchedEvent event) {
				if(event.getState() == Event.KeeperState.SyncConnected) {
				 signal.countDown();
				}
			}
		});
		
		signal.await();
		return zk;
	}
	
	
	public static void main(String[] args) {
		ConnectionTest conn = new ConnectionTest();
		try {
			ZooKeeper zk = conn.connect(args[0]);
			System.out.printf("ZK state: %s\n", zk.getState());
			zk.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
