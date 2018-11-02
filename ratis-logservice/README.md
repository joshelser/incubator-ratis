# Ratis LogService


## Example shell

Build:
```bash
$ mvn install
```

Change to logservice directory:
```bash
$ cd ratis-log-service
```

Launch Metadata daemons:
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.MasterServer -Dexec.args="-port 9991 -dir $HOME/logservice1 -hostname localhost -quorum localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.MasterServer -Dexec.args="-port 9992 -dir $HOME/logservice2 -hostname localhost -quorum localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.server.MasterServer -Dexec.args="-port 9993 -dir $HOME/logservice3 -hostname localhost -quorum localhost:9991,localhost:9992,localhost:9993"
```

Above, we have started three daemons that will form a quorum to act as the "LogService Metadata". They will track what
logs exist and the RAFT quorums which service those logs.

Launch Worker daemons:
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.worker.LogServiceWorker -Dexec.args="-port 9951 -dir $HOME/worker1 -meta localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.worker.LogServiceWorker -Dexec.args="-port 9953 -dir $HOME/worker2 -meta localhost:9991,localhost:9992,localhost:9993"
```
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.worker.LogServiceWorker -Dexec.args="-port 9953 -dir $HOME/worker3 -meta localhost:9991,localhost:9992,localhost:9993"
```

Now, we have started three daemons which can service a single LogStream. They will report to the Metadata quorum,
and the Metadata quorum will choose three of them to form a RAFT quorum to "host" a single Log.

Note: the `meta` option here references to the Metadata quorum, not the worker quorum as is the case for the Metadata daemons.

Launch client:
```bash
$ mvn exec:java -Dexec.mainClass=org.apache.ratis.logservice.shell.LogServiceShell -Dexec.args="-q localhost:9990,localhost:9991,localhost:9992"
```

This command will launch an interactive shell that you can use to interact with the system.
