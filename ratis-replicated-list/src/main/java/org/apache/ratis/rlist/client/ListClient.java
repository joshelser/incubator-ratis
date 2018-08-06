package org.apache.ratis.rlist.client;

import java.io.IOException;

public interface ListClient {

  int size() throws IOException;
  String set(int offset, String value) throws IOException;
  void append(String value) throws IOException;
  void insert(int offset, String value) throws IOException;
  String get(int offset) throws IOException;
  
}
