package raven.client.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UrlUtils {

  private static Log log = LogFactory.getLog(UrlUtils.class.getCanonicalName());

  public static String escapeDataString(String input) {
    try {
      if (input == null) {
        return "";
      }
      return URLEncoder.encode(input, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error(e.getMessage(), e);
      return null;
    }
  }

  public static String unescapeDataString(String input) {
    try {
      if (input == null) {
        return null;
      }
      return URLDecoder.decode(input, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error(e.getMessage(), e);
      return null;
    }

  }
}
