package raven.client.connection;

import org.apache.commons.codec.digest.DigestUtils;

public class ServerHash {

  public static String getServerHash(String url) {
    return DigestUtils.md5Hex(url);
  }

}
