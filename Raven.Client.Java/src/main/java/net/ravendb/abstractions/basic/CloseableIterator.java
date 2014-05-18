package net.ravendb.abstractions.basic;

import java.io.Closeable;
import java.util.Iterator;


public interface CloseableIterator<T> extends Iterator<T>, Closeable {
  @Override
  public void close();
}
