package org.apache.ratis.rlist.client;

import org.apache.ratis.client.RaftClient;

public class ReplicatedListFactory {

  private static final ReplicatedListFactory INSTANCE = new ReplicatedListFactory();

  private ReplicatedListFactory() {}

  public static ReplicatedListFactory getInstance() {
    return INSTANCE;
  }

  public ListClient createClient(RaftClient client) {
    return new ListClientImpl(client);
  }
}
