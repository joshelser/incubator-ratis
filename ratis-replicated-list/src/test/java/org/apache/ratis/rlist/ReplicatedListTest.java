package org.apache.ratis.rlist;

import java.io.IOException;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGRpc;
import org.apache.ratis.rlist.client.ListClient;
import org.apache.ratis.rlist.client.ReplicatedListFactory;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.Before;
import org.junit.Test;

public class ReplicatedListTest extends BaseTest implements MiniRaftClusterWithGRpc.FactoryGet {
  @Before
  public void setupStateMachine() {
    final RaftProperties p = getProperties(); 
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        ListStateMachine.class, StateMachine.class);
  }
  private static final int NUM_PEERS = 3;

  @Test
  public void testBasicReadAndWrite() throws IOException, InterruptedException {
    final MiniRaftClusterWithGRpc cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    try (final RaftClient raftClient = RaftClient.newBuilder()
        .setProperties(getProperties())
        .setRaftGroup(cluster.getGroup())
        .build();) {
      ListClient rlistClient = ReplicatedListFactory.getInstance().createClient(raftClient);
      System.out.println("Size = " + rlistClient.size());
      System.out.println("Appending 'a'");
      rlistClient.append("a");
      System.out.println("Appending 'b'");
      rlistClient.append("b");
      System.out.println("Appending 'c'");
      rlistClient.append("c");
  
      System.out.println("Size = " + rlistClient.size());
      System.out.println("list.get(0) = " + rlistClient.get(0));
      System.out.println("list.get(1) = " + rlistClient.get(1));
      System.out.println("list.get(2) = " + rlistClient.get(2));
  
      System.out.println("list.set(1, 'd') = " + rlistClient.set(1, "d"));
      System.out.println("list.get(1) = " + rlistClient.get(1));
    } finally {
      cluster.shutdown();
    }
  }
}
