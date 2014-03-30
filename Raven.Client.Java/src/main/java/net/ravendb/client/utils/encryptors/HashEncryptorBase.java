package net.ravendb.client.utils.encryptors;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;


public class HashEncryptorBase {
  public byte[] computeHash(String hashAlgorithm, byte[] bytes, Integer size) throws NoSuchAlgorithmException {
    MessageDigest messageDigest = MessageDigest.getInstance(hashAlgorithm);
    byte[] digest = messageDigest.digest(bytes);
    if (size != null) {
      return Arrays.copyOfRange(digest, 0, size);
    } else {
      return digest;
    }
  }
}
