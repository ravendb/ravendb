package net.ravendb.client.utils;

public class Closer {
  public static void close(AutoCloseable objectToClose) {
    try {
      objectToClose.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
