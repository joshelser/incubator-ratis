package org.apache.ratis.rlist;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGRpc;
import org.apache.ratis.rlist.client.ListClient;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.Before;
import org.junit.Test;

public class RListTest extends BaseTest implements MiniRaftClusterWithGRpc.FactoryGet {
  @Before
  public void setupStateMachine() {
    final RaftProperties p = getProperties(); 
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        RListStateMachine.class, StateMachine.class);
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
      ListClient rlistClient = RListFactory.getInstance().createClient(raftClient);
      assertEquals(0, rlistClient.size());
      rlistClient.append("a");
      rlistClient.append("b");
      rlistClient.append("c");
  
      assertEquals(3, rlistClient.size());
      assertEquals("a", rlistClient.get(0));
      assertEquals("b", rlistClient.get(1));
      assertEquals("c", rlistClient.get(2));

      assertEquals("b", rlistClient.set(1, "d"));
      assertEquals("d", rlistClient.get(1));
    } finally {
      cluster.shutdown();
    }
  }
}
