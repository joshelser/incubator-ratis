package org.apache.ratis.rlist;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rlist.client.ListClient;
import org.apache.ratis.rlist.client.ReplicatedListFactory;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;

public class Driver {
  protected static final String raftGroupId = "demoRaftGroup123";

  static RaftClientRpc createGrpcFactory(RaftProperties props) {
    return new GrpcFactory(new Parameters())
        .newRaftClientRpc(ClientId.randomId(), props);
  }

  static RaftClient createRaftClient() {
    RaftProperties raftProperties = new RaftProperties();

    RaftGroup raftGroup = new RaftGroup(RaftGroupId.valueOf(
        ByteString.copyFromUtf8(raftGroupId)),
        new RaftPeer[] {
            new RaftPeer(RaftPeerId.valueOf("n1"), "localhost:60000"),
            new RaftPeer(RaftPeerId.valueOf("n2"), "localhost:60001"),
            new RaftPeer(RaftPeerId.valueOf("n3"), "localhost:60002")
        });

    RaftClient.Builder builder =
        RaftClient.newBuilder().setProperties(raftProperties);
    builder.setRaftGroup(raftGroup);
    builder.setClientRpc(createGrpcFactory(raftProperties));
    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    RaftClient raftClient = createRaftClient();

    try {
      ListClient listClient = ReplicatedListFactory.getInstance().createClient(raftClient);
      System.out.println("Size = " + listClient.size());
      System.out.println("Appending 'a'");
      listClient.append("a");
      System.out.println("Appending 'b'");
      listClient.append("b");
      System.out.println("Appending 'c'");
      listClient.append("c");
  
      System.out.println("Size = " + listClient.size());
      System.out.println("list.get(0) = " + listClient.get(0));
      System.out.println("list.get(1) = " + listClient.get(1));
      System.out.println("list.get(2) = " + listClient.get(2));
  
      System.out.println("list.set(1, 'd') = " + listClient.set(1, "d"));
      System.out.println("list.get(1) = " + listClient.get(1));
    } finally {
      raftClient.close();
    }
  }
}
