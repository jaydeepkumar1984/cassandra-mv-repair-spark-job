package mvsync.output;

public class ConsoleStreamer implements IBlobStreamer {

  @Override
  public void append(String data) {
    System.out.println(data);
  }

  @Override
  public void commit() {

  }
}
