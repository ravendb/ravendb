package net.ravendb.client.utils.encryptors;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;


public class FipsSymmetricalEncryptor  implements ISymmetricalEncryptor {

  private SecureRandom random = new SecureRandom();

  private SecretKey secretKey;

  private byte[] iv;

  @Override
  public byte[] getKey() {
    return secretKey.getEncoded();
  }

  @Override
  public byte[] getIV() {
    return iv;
  }

  @Override
  public int getKeySize() {
    return 256;
  }

  @Override
  public void generateKey() throws NoSuchAlgorithmException {
    KeyGenerator kgen = KeyGenerator.getInstance("AES");
    kgen.init(256);
    secretKey = kgen.generateKey();
  }

  @Override
  public void generateIV() {
    byte[] iv = new byte[16];
    random.nextBytes(iv);
    this.iv = iv;
  }

  @Override
  public Cipher createEncryptor() throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException {
    Cipher aes = Cipher.getInstance("AES/CBC/PKCS7Padding");
    IvParameterSpec ivSpec = new IvParameterSpec(iv);
    aes.init(Cipher.ENCRYPT_MODE, secretKey, ivSpec);
    return aes;
  }

}
