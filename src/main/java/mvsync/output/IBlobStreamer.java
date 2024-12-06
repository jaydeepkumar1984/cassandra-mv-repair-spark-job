package mvsync.output;

import java.io.IOException;
import java.io.Serializable;

public interface IBlobStreamer extends Serializable {

  void append(String data) throws IOException;

  void commit() throws IOException;
}
