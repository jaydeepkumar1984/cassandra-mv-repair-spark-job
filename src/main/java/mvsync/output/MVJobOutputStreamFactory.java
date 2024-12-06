package mvsync.output;

import mvsync.MVSyncSettings;

import java.io.Serializable;

public class MVJobOutputStreamFactory implements Serializable {
  public IBlobStreamer getStream(String path, MVSyncSettings mvSyncSettings) throws Exception {
    return new FileStreamer(path);
  }
}
