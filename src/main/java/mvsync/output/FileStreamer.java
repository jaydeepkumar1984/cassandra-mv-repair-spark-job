package mvsync.output;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

public class FileStreamer implements IBlobStreamer {
  Path filePath;
  boolean initialized;
  public FileStreamer(String path) throws IOException {
    filePath = Paths.get(path);
  }

  @Override
  public void append(String data) throws IOException {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          System.out.println("FilePath: " + filePath.getFileName());
          Files.createDirectories(filePath.getParent());
          if (Files.exists(filePath)) {
            Files.delete(filePath);
          }
          Files.createFile(filePath);
          initialized = true;
        }
      }
    }
    Files.write(filePath, Collections.singletonList(data),
            StandardOpenOption.APPEND);
  }

  @Override
  public void commit() {

  }
}
