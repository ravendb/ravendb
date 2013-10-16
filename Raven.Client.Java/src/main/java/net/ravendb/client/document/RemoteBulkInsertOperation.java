package net.ravendb.client.document;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.BulkInsertChangeNotification;
import net.ravendb.abstractions.data.BulkInsertOptions;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.DocumentChangeTypes;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.changes.IObserver;
import net.ravendb.client.connection.ServerClient;
import net.ravendb.client.connection.implementation.HttpJsonRequest;
import net.ravendb.client.utils.CancellationTokenSource;
import net.ravendb.client.utils.CancellationTokenSource.CancellationToken;

import org.apache.commons.lang.ArrayUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;

import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonGenerator;


public class RemoteBulkInsertOperation implements ILowLevelBulkInsertOperation, IObserver<BulkInsertChangeNotification> {

  private final BsonFactory bsonFactory = new BsonFactory();

  private final static RavenJObject END_OF_QUEUE_OBJECT = RavenJObject.parse("{ \"QueueFinished\" : true }");

  private CancellationTokenSource cancellationTokenSource;
  private final ServerClient operationClient;

  private final ByteArrayOutputStream bufferedStream = new ByteArrayOutputStream();
  private final BlockingQueue<RavenJObject> queue;

  private HttpJsonRequest operationRequest;
  private byte[] responseBytes;
  private final Thread operationTask;
  private Exception operationTaskException;
  private int total;

  private Action1<String> report;
  private UUID operationId;
  private transient boolean disposed;

  @Override
  public UUID getOperationId() {
    return operationId;
  }

  @Override
  public Action1<String> getReport() {
    return report;
  }

  @Override
  public void setReport(Action1<String> report) {
    this.report = report;
  }



  public RemoteBulkInsertOperation(BulkInsertOptions options, ServerClient client, IDatabaseChanges changes) {
    operationId = UUID.randomUUID();
    operationClient = client;
    queue = new ArrayBlockingQueue<>(Math.max(128, (options.getBatchSize() * 3) / 2));

    operationTask = startBulkInsertAsync(options);
    subscribeToBulkInsertNotifications(changes);
  }

  private void subscribeToBulkInsertNotifications(IDatabaseChanges changes) {
    changes.forBulkInsert(operationId).subscribe(this);
  }

  private class BulkInsertEntity implements HttpEntity {

    private BulkInsertOptions options;
    private CancellationToken cancellationToken;

    public BulkInsertEntity(BulkInsertOptions options, CancellationToken cancellationToken) {
      this.options = options;
      this.cancellationToken = cancellationToken;
    }

    @Override
    public boolean isRepeatable() {
      return false;
    }

    @Override
    public boolean isChunked() {
      return true;
    }

    @Override
    public long getContentLength() {
      return 0;
    }

    @Override
    public Header getContentType() {
      return null;
    }

    @Override
    public Header getContentEncoding() {
      return null;
    }

    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
      throw new IllegalStateException("Not supported!");
    }

    @Override
    public void writeTo(OutputStream outstream) throws IOException {
      writeQueueToServer(outstream, options, cancellationToken);
    }

    @Override
    public boolean isStreaming() {
      return true;
    }

    @Deprecated
    @Override
    public void consumeContent() throws IOException {
      //empty
    }

  }

  private CancellationToken createCancellationToken() {
    cancellationTokenSource = new CancellationTokenSource();
    return cancellationTokenSource.getToken();
  }

  private Thread startBulkInsertAsync(BulkInsertOptions options) {
    operationClient.setExpect100Continue(true);

    String operationUrl = createOperationUrl(options);
    String token = getToken();
    try {
      token = validateThatWeCanUseAuthenticateTokens(token);
    } catch (Exception e) {
      throw new IllegalStateException("Could not authenticate token for bulk insert, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration", e);
    }

    operationRequest = createOperationRequest(operationUrl, token);
    HttpPost webRequest = (HttpPost) operationRequest.getWebRequest();
    webRequest.setEntity(new BulkInsertEntity(options, createCancellationToken()));


    Thread thread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          responseBytes = operationRequest.readResponseBytes();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    operationClient.setExpect100Continue(false);
    thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        operationTaskException = (Exception) e;
      }
    });

    thread.start();
    return thread;

  }

  private String getToken() {
    RavenJToken jsonToken = getAuthToken();
    return jsonToken.value(String.class, "Token");
  }

  private RavenJToken getAuthToken() {
    HttpJsonRequest request = operationClient.createRequest(HttpMethods.GET, "/singleAuthToken", true);
    return request.readResponseJson();
  }

  private String validateThatWeCanUseAuthenticateTokens(String token) {
    HttpJsonRequest request = operationClient.createRequest(HttpMethods.GET, "/singleAuthToken", true);
    request.removeAuthorizationHeader();
    request.addOperationHeader("Single-Use-Auth-Token", token);
    RavenJToken result = request.readResponseJson();
    return result.value(String.class, "Token");
  }

  private HttpJsonRequest createOperationRequest(String operationUrl, String token) {
    HttpJsonRequest request = operationClient.createRequest(HttpMethods.POST, operationUrl, true);

    request.prepareForLongRequest();
    request.addOperationHeader("Single-Use-Auth-Token", token);

    return request;
  }

  private String createOperationUrl(BulkInsertOptions options) {
    String requestUrl = "/bulkInsert?";
    if (options.isCheckForUpdates()) {
      requestUrl += "checkForUpdates=true";
    }
    if (options.isCheckReferencesInIndexes()) {
      requestUrl += "&checkReferencesInIndexes=true";
    }

    requestUrl += "&operationId=" + operationId;

    return requestUrl;
  }
  private void writeQueueToServer(OutputStream stream, BulkInsertOptions options, CancellationToken cancellationToken) throws IOException {

    while (true) {
      cancellationToken.throwIfCancellationRequested();
      List<RavenJObject> batch = new ArrayList<>();
      try {
        RavenJObject document;
        while ((document = queue.poll(200, TimeUnit.MICROSECONDS)) != null) {
          cancellationToken.throwIfCancellationRequested();

          if (document == END_OF_QUEUE_OBJECT) { //marker
            flushBatch(stream, batch);
            return;
          }
          batch.add(document);

          if (batch.size() >= options.getBatchSize()) {
            break;
          }
        }
      } catch (InterruptedException e ){
        //ignore
      }
      flushBatch(stream, batch);
    }
  }

  @Override
  public void write(String id, RavenJObject metadata, RavenJObject data) {
    if (id == null) {
      throw new IllegalArgumentException("id");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("metadata");
    }
    if (data == null) {
      throw new IllegalArgumentException("data");
    }

    if (operationTaskException != null) {
      throw new RuntimeException(operationTaskException); // error early if we have  any error
    }

    metadata.add("@id", id);
    data.add(Constants.METADATA, metadata);
    try {
      while (!queue.offer(data)) {
        Thread.sleep(250);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }


  private boolean isOperationCompleted(long operationId) {
    RavenJToken status = getOperationStatus(operationId);
    if (status == null) {
      return true;
    }
    if (status.value(Boolean.class, "Completed")) {
      return true;
    }
    return false;
  }

  private RavenJToken getOperationStatus(long operationId) {
    return operationClient.getOperationStatus(operationId);
  }


  @Override
  public void close() throws InterruptedException {
    if (disposed) {
      return ;
    }
    queue.add(END_OF_QUEUE_OBJECT);
    operationTask.join();

    if (operationTaskException != null) {
      throw new RuntimeException(operationTaskException);
    }

    reportInternal("Finished writing all results to server");

    long operationId = 0;

    RavenJObject result = RavenJObject.parse(new String(responseBytes));
    operationId = result.value(Long.class, "OperationId");

    while (true) {
      if (isOperationCompleted(operationId)) {
        break;
      }
      Thread.sleep(500);
    }
    reportInternal("Done writing to server");
  }

  private  void flushBatch(OutputStream requestStream, Collection<RavenJObject> localBatch) throws IOException {
    if (localBatch.isEmpty()) {
      return ;
    }
    bufferedStream.reset();
    writeToBuffer(localBatch);

    byte[] bytes = ByteBuffer.allocate(4).putInt(bufferedStream.size()).array();
    ArrayUtils.reverse(bytes);
    requestStream.write(bytes);
    bufferedStream.writeTo(requestStream);
    requestStream.flush();

    total += localBatch.size();

    Action1<String> report = getReport();
    if (report != null) {
      report.apply(String.format("Wrote %d (total %d) documents to server gzipped to %d kb", localBatch.size(), total, bufferedStream.size() / 1024));
    }

  }

  private void writeToBuffer(Collection<RavenJObject> localBatch) throws IOException {
    GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bufferedStream);

    BsonGenerator bsonWriter = bsonFactory.createJsonGenerator(gzipOutputStream);
    bsonWriter.disable(org.codehaus.jackson.JsonGenerator.Feature.AUTO_CLOSE_TARGET);

    byte[] bytes = ByteBuffer.allocate(4).putInt(localBatch.size()).array();
    ArrayUtils.reverse(bytes);
    gzipOutputStream.write(bytes);
    for (RavenJObject doc : localBatch) {
      doc.writeTo(bsonWriter);
    }
    bsonWriter.close();
    gzipOutputStream.finish();
    bufferedStream.flush();
  }

  private void reportInternal(String format, Object... args) {
    Action1<String> onReport = report;
    if (onReport != null) {
      onReport.apply(String.format(format, args));
    }
  }

  @Override
  public void onNext(BulkInsertChangeNotification value) {
    if (value.getType().equals(DocumentChangeTypes.BULK_INSERT_ERROR)) {
      cancellationTokenSource.cancel();
    }
  }

  @Override
  public void onError(Exception error) {
    //empty by design
  }

  @Override
  public void onCompleted() {
    //empty by design
  }
}
