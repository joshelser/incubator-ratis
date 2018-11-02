/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.logservice.server;

import static org.apache.ratis.logservice.common.Constants.metaGroupID;
import static org.apache.ratis.logservice.util.LogServiceUtils.getPeersFromQuorum;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;

import com.beust.jcommander.JCommander;

/**
 * Master quorum is responsible for tracking all available quorum members
 */
public class MasterServer implements Closeable {

    // RaftServer internal server. Has meta raft group and MetaStateMachine
    private  RaftServer server;

    private String id;

    private ServerOpts opts;

    private StateMachine metaStateMachine;

    private LifeCycle lifeCycle;

    private static ServerOpts buildOpts(String hostname, int port, String workingDir) {
      ServerOpts opts = new ServerOpts();
      opts.host = hostname;
      opts.port = port;
      opts.workingDir = workingDir;
      return opts;
    }

    public MasterServer(ServerOpts opts) {
      this.opts = opts;
      this.id = opts.host + "_" + opts.port;
      this.lifeCycle = new LifeCycle(this.id);
    }

    public MasterServer(String hostname, int port, String workingDir) {
        this(buildOpts(hostname, port, workingDir));
    }

    public MasterServer() {

    }

    public void start(String metaGroupId) throws IOException  {
        if (opts.host == null) {
          opts.host = LogServiceUtils.getHostName();
        }
        if (id == null) {
          id = opts.host + "_" + opts.port;
        }
        this.lifeCycle = new LifeCycle(this.id);
        RaftProperties properties = new RaftProperties();
        if(opts.workingDir != null) {
            RaftServerConfigKeys.setStorageDirs(properties, Collections.singletonList(new File(opts.workingDir)));
        }
        GrpcConfigKeys.Server.setPort(properties, opts.port);
        NettyConfigKeys.Server.setPort(properties, opts.port);
        Set<RaftPeer> peers = getPeersFromQuorum(metaGroupId);
        RaftGroup metaGroup = RaftGroup.valueOf(Constants.metaGroupID, peers);
        metaStateMachine = new MetaStateMachine();
        server = RaftServer.newBuilder()
                .setGroup(metaGroup)
                .setServerId(RaftPeerId.valueOf(id))
                .setStateMachineRegistry(raftGroupId -> {
                    if(raftGroupId.equals(metaGroupID)) {
                        return metaStateMachine;
                    }
                    return null;
                })
                .setProperties(properties).build();
        lifeCycle.startAndTransition(() -> {
            server.start();
        }, IOException.class);
    }

    public static void main(String[] args) throws IOException {
        ServerOpts opts = new ServerOpts();
        JCommander.newBuilder()
                .addObject(opts)
                .build()
                .parse(args);
        System.out.println(opts);
        MasterServer master = new MasterServer(opts);
        master.start(opts.metaQuorum);
        while (true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return;
          }
        }
    }

    public static MasterServer.Builder newBuilder() {
        return new MasterServer.Builder();
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    public String getId() {
        return id;
    }

    public String getAddress() {
        return opts.host + ":" + opts.port;
    }

    public void cleanUp() throws IOException {
        FileUtils.deleteFully(new File(opts.workingDir));
    }

    public static class Builder {
        private String host = null;
        private int port = 9999;
        private String workingDir = null;

        /**
         * @return a {@link MasterServer} object.
         */
        public MasterServer build()  {
            if (host == null) {
                host = LogServiceUtils.getHostName();
            }
            return new MasterServer(host, port, workingDir);
        }

        /**
         * Set the server hostname.
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Set server port
         */
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setWorkingDir(String workingDir) {
            this.workingDir = workingDir;
            return this;
        }
    }
}
