package org.apache.ratis.rlist;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.rlist.RListProtos.RaftRequest;

public class RListMessage implements Message {

  final ByteString data;

  public RListMessage(RaftRequest protoRequest) {
    this.data = protoRequest.toByteString();
  }

  public RListMessage(ByteString data) {
    this.data = data;
  }

  @Override
  public ByteString getContent() {
    return data;
  }
}
