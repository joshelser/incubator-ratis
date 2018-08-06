package org.apache.ratis.rlist;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.rlist.client.ListClient;
import org.apache.ratis.rlist.client.impl.ListClientImpl;

public class RListFactory {

  private static final RListFactory INSTANCE = new RListFactory();

  private RListFactory() {}

  public static RListFactory getInstance() {
    return INSTANCE;
  }

  public ListClient createClient(RaftClient client) {
    return new ListClientImpl(client);
  }
}
