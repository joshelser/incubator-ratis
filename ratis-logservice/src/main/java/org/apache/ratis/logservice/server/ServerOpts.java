package org.apache.ratis.logservice.server;

import com.beust.jcommander.Parameter;

public class ServerOpts {
  @Parameter(names = {"-h", "--hostname"}, description = "Hostname")
  public String host;

  @Parameter(names = {"-p", "--port"}, description = "Port number")
  public int port = 9999;

  @Parameter(names = {"-d", "--dir"}, description = "Working directory")
  public String workingDir = null;

  @Parameter(names = {"-q", "--metaQuorum"}, description = "Metadata Service Quorum")
  public String metaQuorum;

  @Override
  public String toString() {
    return "Hostname=" + host + ", port=" + port + ", dir=" + workingDir + ", metaQuorum=" + metaQuorum;
  }
}
