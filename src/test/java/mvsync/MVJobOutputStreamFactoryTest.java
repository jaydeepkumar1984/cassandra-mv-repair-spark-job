package mvsync;

import mvsync.output.ConsoleStreamer;
import mvsync.output.IBlobStreamer;
import mvsync.output.MVJobOutputStreamFactory;

public class MVJobOutputStreamFactoryTest extends MVJobOutputStreamFactory {
  @Override
  public IBlobStreamer getStream(String path, MVSyncSettings mvSyncSettings) {
    return new ConsoleStreamer();
  }
}
