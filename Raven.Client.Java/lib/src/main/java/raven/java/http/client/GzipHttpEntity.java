package raven.java.http.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;

public class GzipHttpEntity implements HttpEntity {
  private HttpEntity inner;

  public GzipHttpEntity(HttpEntity inner) {
    super();
    this.inner = inner;
  }

  /**
   * @return
   * @see org.apache.http.HttpEntity#isRepeatable()
   */
  public boolean isRepeatable() {
    return inner.isRepeatable();
  }

  /**
   * @return
   * @see org.apache.http.HttpEntity#isChunked()
   */
  public boolean isChunked() {
    return inner.isChunked();
  }

  /**
   * @return
   * @see org.apache.http.HttpEntity#getContentLength()
   */
  public long getContentLength() {
    return inner.getContentLength();
  }

  /**
   * @return
   * @see org.apache.http.HttpEntity#getContentType()
   */
  public Header getContentType() {
    return inner.getContentType();
  }

  /**
   * @return
   * @see org.apache.http.HttpEntity#getContentEncoding()
   */
  public Header getContentEncoding() {
    return inner.getContentEncoding();
  }

  /**
   * @return
   * @throws IOException
   * @throws IllegalStateException
   * @see org.apache.http.HttpEntity#getContent()
   */
  public InputStream getContent() throws IOException, IllegalStateException {
    return inner.getContent();
  }

  /**
   * @param outstream
   * @throws IOException
   * @see org.apache.http.HttpEntity#writeTo(java.io.OutputStream)
   */
  public void writeTo(OutputStream outstream) throws IOException {
    GZIPOutputStream gzipOs = new GZIPOutputStream(outstream);
    inner.writeTo(gzipOs);
    gzipOs.finish();
  }

  /**
   * @return
   * @see org.apache.http.HttpEntity#isStreaming()
   */
  public boolean isStreaming() {
    return inner.isStreaming();
  }

  /**
   * @throws IOException
   * @deprecated
   * @see org.apache.http.HttpEntity#consumeContent()
   */
  public void consumeContent() throws IOException {
    inner.consumeContent();
  }



}
