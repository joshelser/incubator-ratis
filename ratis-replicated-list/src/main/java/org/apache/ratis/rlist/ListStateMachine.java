package org.apache.ratis.rlist;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.TextFormat;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.shaded.proto.rlist.RListProtos;
import org.apache.ratis.shaded.proto.rlist.RListProtos.AppendRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.AppendResponse;
import org.apache.ratis.shaded.proto.rlist.RListProtos.GetRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.GetResponse;
import org.apache.ratis.shaded.proto.rlist.RListProtos.InsertRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.InsertResponse;
import org.apache.ratis.shaded.proto.rlist.RListProtos.RaftRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.RaftResponse;
import org.apache.ratis.shaded.proto.rlist.RListProtos.SetRequest;
import org.apache.ratis.shaded.proto.rlist.RListProtos.SetResponse;
import org.apache.ratis.shaded.proto.rlist.RListProtos.SizeResponse;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListStateMachine extends BaseStateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(ListStateMachine.class);

  private final SimpleStateMachineStorage smStorage = new SimpleStateMachineStorage();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final List<String> data = new LinkedList<>();

  private AutoCloseableLock readLock() {
    return AutoCloseableLock.acquire(lock.readLock());
  }

  private AutoCloseableLock writeLock() {
    return AutoCloseableLock.acquire(lock.writeLock());
  }

  void reset() {
    data.clear();
    setLastAppliedTermIndex(null);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    smStorage.init(raftStorage);
    loadSnapshot(smStorage.getLatestSnapshot());
  }

  long loadSnapshot(SingleFileSnapshotInfo snapshot) throws IOException {
    if (snapshot == null) {
      LOG.warn("SnapshotInfo was null");
      return RaftServerConstants.INVALID_LOG_INDEX;
    }
    
    final File snapshotFile = snapshot.getFile().getPath().toFile();
    if (!snapshotFile.exists()) {
      LOG.warn("Snapshot file {} is missing {}", snapshotFile, snapshot);
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    // Extract the TermIndex from the snapshot file's name
    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
    try (AutoCloseableLock writeLock = writeLock()) {
      setLastAppliedTermIndex(last);
      try (InputStream in = new BufferedInputStream(new FileInputStream(snapshotFile))) {
        RListProtos.ListProto protoList = RListProtos.ListProto.parseFrom(in);
        protoList.getDataList().stream().map((bytes) -> data.add(bytes.toStringUtf8()));
      }
    }
    return last.getIndex();
  }

  @Override
  public long takeSnapshot() throws IOException {
    final List<String> copy;
    final TermIndex last;
    try (AutoCloseableLock readLock = readLock()) {
      copy = new LinkedList<>(data);
      last = getLastAppliedTermIndex();
    }

    final File snapshotFile = smStorage.getSnapshotFile(last.getTerm(), last.getIndex());
    LOG.info("Snapshotting into file {}", snapshotFile);

    RListProtos.ListProto.Builder builder = RListProtos.ListProto.newBuilder();
    copy.stream().map((str) -> builder.addData(ByteString.copyFromUtf8(str)));
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(snapshotFile))) {
      builder.build().writeTo(out);
    }

    return last.getIndex();
  }

  @Override
  public SimpleStateMachineStorage getStateMachineStorage() {
    return smStorage;
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    RaftRequest raftRequest;
    try {
      raftRequest = RaftRequest.parseFrom(request.getContent());
    } catch (IOException e) {
      return JavaUtils.completeExceptionally(e);
    }
    if (raftRequest.hasGet()) {
      // get[x]
      final GetRequest protoGet = raftRequest.getGet();
      final int index = protoGet.getIndex();
      if (index < 0) {
        return JavaUtils.completeExceptionally(new IOException("Index for Get request cannot be negative"));
      }
      final String dataAtIndex;
      try (AutoCloseableLock rlock = readLock()) {
        int currentSize = data.size();
        if (index >= currentSize) {
          return JavaUtils.completeExceptionally(new IOException("Get requested for index " + index + " but size is " + currentSize));
        }
        dataAtIndex = data.get(index);
      }
      GetResponse getResp = GetResponse.newBuilder()
          .setData(ByteString.copyFromUtf8(dataAtIndex))
          .build();
      Message msg = Message.valueOf(RaftResponse.newBuilder()
          .setGet(getResp)
          .build().toByteString());
      return CompletableFuture.completedFuture(msg);
    } else if (raftRequest.hasSize()) {
      // size()
      final int size;
      try (AutoCloseableLock rlock = readLock()) {
        size = data.size();
      }
      SizeResponse sizeResp = SizeResponse.newBuilder()
          .setSize(size)
          .build();
      Message resp = Message.valueOf(RaftResponse.newBuilder()
          .setSize(sizeResp)
          .build().toByteString());
      return CompletableFuture.completedFuture(resp);
    } else {
      // Unknown...
      LOG.error(getId() + ": Unexpected request case " + raftRequest.toString());
      return JavaUtils.completeExceptionally(new IOException("Failed to process message"));
    }
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext txn) {
    final LogEntryProto entry = txn.getLogEntry();
    final ByteString requestData = entry.getSmLogEntry().getData();
    RaftRequest raftRequest;
    try {
      raftRequest = RaftRequest.parseFrom(requestData);
    } catch (IOException e) {
      return JavaUtils.completeExceptionally(e);
    }
    if (raftRequest.hasInsert()) {
      final InsertRequest req = raftRequest.getInsert();
      final int insertionIndex = req.getIndex();
      final String insertionData = req.getData().toStringUtf8();
      try (AutoCloseableLock writeLock = writeLock()) {
        data.add(insertionIndex, insertionData);
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      }
      Message resp = Message.valueOf(RaftResponse.newBuilder()
          .setInsert(InsertResponse.getDefaultInstance())
          .build()
          .toByteString());
      return CompletableFuture.completedFuture(resp);
    } else if (raftRequest.hasSet()) {
      final SetRequest req = raftRequest.getSet();
      final int setIndex = req.getIndex();
      final String setData = req.getData().toStringUtf8();
      final String oldData;
      try (AutoCloseableLock writeLock = writeLock()) {
        oldData = data.set(setIndex, setData);
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      }
      SetResponse setResp = SetResponse.newBuilder().
          setPrevData(ByteString.copyFromUtf8(oldData))
          .build();
      Message resp = Message.valueOf(RaftResponse.newBuilder()
          .setSet(setResp)
          .build()
          .toByteString());
      return CompletableFuture.completedFuture(resp);
    } else if (raftRequest.hasAppend()) {
      final AppendRequest req = raftRequest.getAppend();
      final String appendData = req.getData().toStringUtf8();
      try (AutoCloseableLock writeLock = writeLock()) {
        data.add(appendData);
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      }
      Message resp = Message.valueOf(RaftResponse.newBuilder()
          .setAppend(AppendResponse.getDefaultInstance())
          .build()
          .toByteString());
      return CompletableFuture.completedFuture(resp);
    } else {
      LOG.error(getId() + ": Unexpected request case " + TextFormat.shortDebugString(raftRequest));
      return JavaUtils.completeExceptionally(new IOException("Unknown message type"));
    }
  }

  @Override
  public void close() {
    reset();
  }

  @Override
  public void reinitialize() throws IOException {
    close();
    loadSnapshot(smStorage.getLatestSnapshot());
  }
}
