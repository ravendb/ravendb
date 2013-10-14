package net.ravendb.abstractions.connection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

public class HttpRequestHelper {

  public static void copyHeaders(HttpRequest src, HttpRequest dest) {
    for (Header header: src.getAllHeaders()) {
      dest.addHeader(header);
    }
  }

  public static void writeDataToRequest(HttpRequest newWebRequest, String postedData, boolean disableRequestCompression) {

    HttpEntityEnclosingRequestBase requestMethod = (HttpEntityEnclosingRequestBase) newWebRequest;

    try {
      if (disableRequestCompression) {
        StringEntity entity = new StringEntity(postedData, ContentType.APPLICATION_JSON);
        entity.setChunked(true);
        requestMethod.setEntity(entity);
      } else {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOS = new GZIPOutputStream(baos);
        IOUtils.write(postedData, gzipOS, Consts.UTF_8.name());
        IOUtils.closeQuietly(gzipOS);
        ByteArrayEntity entity = new ByteArrayEntity(baos.toByteArray(), ContentType.APPLICATION_JSON);
        entity.setChunked(true);
        requestMethod.setEntity(entity);
      }

    } catch (IOException e) {
      throw new RuntimeException("Unable to gzip data!", e);
    }

  }
}
