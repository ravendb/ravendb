package net.ravendb.client.connection;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import net.ravendb.abstractions.closure.Action0;
import net.ravendb.abstractions.closure.Predicate;
import net.ravendb.client.changes.IObservable;
import net.ravendb.client.changes.IObserver;
import net.ravendb.client.connection.profiling.ConcurrentSet;

import org.apache.commons.io.IOUtils;



public class ObservableLineStream implements IObservable<String>, Closeable {

  private final InputStream stream;
  private final byte[] buffer = new byte[8192];
  private int posInBuffer;
  private final Action0 onDispose;
  private Thread task;

  private final ConcurrentSet<IObserver<String>> subscribers = new ConcurrentSet<>();

  public ObservableLineStream(InputStream stream, Action0 onDispose) {
    this.stream = stream;
    this.onDispose = onDispose;
  }

  public void start() {
    task = new Thread(new Runnable() {

      @Override
      public void run() {

        while (true) {
          try {
            int read = read();
            if (read == -1) { // will force reopening of the connection
              throw new EOFException();
            }

            // find \r\n in newly read range

            int startPos = 0;
            byte prev = 0;
            boolean foundLines = false;
            for (int i = posInBuffer; i < posInBuffer + read; i++) {
              if (prev == '\r' && buffer[i] == '\n') {
                foundLines = true;
                int oldStartPos = startPos;
                // yeah, we found a line, let us give it to the users
                startPos = i + 1;

                // is it an empty line?
                if (oldStartPos == i -2) {
                  continue; //ignore and continue
                }
                // first 5 bytes should be: 'd','a','t','a',':'
                // if it isn't, ignore and continue
                if (buffer.length - oldStartPos < 5 ||
                  buffer[oldStartPos] != 'd' ||
                  buffer[oldStartPos + 1] != 'a' ||
                  buffer[oldStartPos + 2] != 't' ||
                  buffer[oldStartPos + 3] != 'a' ||
                  buffer[oldStartPos + 4] != ':') {
                  continue;
                }

                String data = new String(buffer,  oldStartPos + 5, i - oldStartPos - 6, Charset.forName("UTF-8"));
                for (IObserver<String> subscriber : subscribers) {
                  subscriber.onNext(data);
                }
              }
              prev = buffer[i];
            }

            posInBuffer += read;
            if (startPos >= posInBuffer) { //read to end
              posInBuffer = 0;
              continue;
            }
            if (!foundLines) {
              continue ;
            }
            // move remaining to the start of buffer, then reset
            System.arraycopy(buffer, startPos, buffer, 0, posInBuffer -startPos);
            posInBuffer -= startPos;
          } catch (Exception e) {
            IOUtils.closeQuietly(stream);
            if (e instanceof EOFException) {
              return ;
            }
            for (IObserver<String> subscriber : subscribers) {
              subscriber.onError(e);
            }
            return;
          }
        }
      }

    }, "ObservableLineStream");
    task.setDaemon(true);
    task.start();
  }

  public Thread getTask() {
    return task;
  }

  public int read() throws IOException {
    return stream.read(buffer, posInBuffer, buffer.length - posInBuffer);
  }

  @Override
  public void close() throws IOException {
    for (IObserver<String> subscriber : subscribers) {
      subscriber.onCompleted();
    }
    onDispose.apply();
  }

  @Override
  public Closeable subscribe(final IObserver<String> observer) {
    subscribers.add(observer);

    return new Closeable() {

      @Override
      public void close() throws IOException {
        subscribers.remove(observer);
      }
    };
  }

  @Override
  public IObservable<String> where(Predicate<String> predicate) {
    throw new UnsupportedOperationException("You can't use ObservableLineStream with where predicate");
  }
}
