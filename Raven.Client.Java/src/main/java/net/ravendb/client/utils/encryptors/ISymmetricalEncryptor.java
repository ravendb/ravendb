package net.ravendb.client.utils.encryptors;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;


public interface ISymmetricalEncryptor {
  public byte[] getKey();
  public byte[] getIV();

  public int getKeySize();
  public void generateKey() throws NoSuchAlgorithmException;
  public void generateIV();

  public Cipher createEncryptor() throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException;
}
