package net.ravendb.client.utils;

import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

/**
 * Class responsible for checking system requirements required by some of RavenDB client features
 */
public class RequirementsChecker {
  /**
   * JCE + Bouncy Castle is required for OAuth requests
   */
  public static void checkOAuthDeps() {
    checkKeySize();
    checkAesCbcPkcs7Padding();
    checkRsaOaepWithSha1();
  }


  private static void checkRsaOaepWithSha1() {
    try {
      Cipher.getInstance("RSA/None/OAEPWITHSHA-1ANDMGF1PADDING");
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new RuntimeException("Please make sure Bouncy Castle is installed as your security provider." , e);
    }
  }


  private static void checkAesCbcPkcs7Padding() {
    try {
      Cipher.getInstance("AES/CBC/PKCS7Padding");
    } catch (NoSuchAlgorithmException| NoSuchPaddingException e) {
      throw new RuntimeException("Please make sure Bouncy Castle is installed as your security provider." , e);
    }
  }


  private static void checkKeySize() {
    try {
      int maxAllowedKeyLength = Cipher.getMaxAllowedKeyLength("AES/CBC/PKCS7Padding");
      if (maxAllowedKeyLength < 256) {
        throw new RuntimeException("Please make sure Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files are installed");
      }
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Please make sure Bouncy Castle is installed as your security provider." , e);
    }
  }
}
