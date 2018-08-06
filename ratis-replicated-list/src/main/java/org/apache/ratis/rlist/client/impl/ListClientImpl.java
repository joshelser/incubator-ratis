package org.apache.ratis.rlist.client.impl;

import java.io.IOException;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.rlist.client.ListClient;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.proto.rlist.RListProtos.AppendRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.GetRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.InsertRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.RaftRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.RaftResponse;
import org.apache.ratis.shaded.proto.rlist.RListProtos.SetRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.SizeRequest;

/**
 * Implementation of {@link ListClient} that sends the corresponding protobuf
 * message to the ListStateMachine the RaftClient, and can unwrap the protobuf
 * response to send the necessary value back to the caller.
 */
public class ListClientImpl implements ListClient {

  private final RaftClient raftClient;

  public ListClientImpl(RaftClient raftClient) {
    this.raftClient = raftClient;
  }

  RaftResponse sendRaftMessage(RaftRequest request) throws IOException {
    RaftClientReply reply = raftClient.send(Message.valueOf(request.toByteString()));
    if (reply.isSuccess()) {
      return RaftResponse.parseFrom(reply.getMessage().getContent());
    }
    throw new RuntimeException("Call failed: " + reply.toString());
  }

  RaftResponse sendReadOnlyRaftMessage(RaftRequest request) throws IOException {
    RaftClientReply reply = raftClient.sendReadOnly(Message.valueOf(request.toByteString()));
    if (reply.isSuccess()) {
      return RaftResponse.parseFrom(reply.getMessage().getContent());
    }
    throw new RuntimeException("Call failed: " + reply.toString());
  }

  @Override
  public int size() throws IOException {
    RaftResponse response = sendReadOnlyRaftMessage(
        RaftRequest.newBuilder().setSize(SizeRequest.getDefaultInstance()).build());
    return response.getSize().getSize();
  }

  @Override
  public String set(int offset, String value) throws IOException {
    RaftResponse response = sendRaftMessage(RaftRequest.newBuilder()
        .setSet(SetRequest.newBuilder()
            .setIndex(offset)
            .setData(ByteString.copyFromUtf8(value))
            .build())
        .build());
    return response.getSet().getPrevData().toStringUtf8();

  }

  @Override
  public void append(String value) throws IOException {
    RaftRequest req = RaftRequest.newBuilder()
        .setAppend(AppendRequest.newBuilder().setData(ByteString.copyFromUtf8(value)).build())
        .build();
    sendRaftMessage(req);
  }

  @Override
  public void insert(int offset, String value) throws IOException {
    RaftRequest req = RaftRequest.newBuilder()
        .setInsert(InsertRequest.newBuilder()
            .setIndex(offset)
            .setData(ByteString.copyFromUtf8(value))
            .build())
        .build();
    sendRaftMessage(req);
  }

  @Override
  public String get(int offset) throws IOException {
    RaftRequest req = RaftRequest.newBuilder()
        .setGet(GetRequest.newBuilder().setIndex(offset).build())
        .build();
    RaftResponse response = sendReadOnlyRaftMessage(req);
    return response.getGet().getData().toStringUtf8();
  }
}
