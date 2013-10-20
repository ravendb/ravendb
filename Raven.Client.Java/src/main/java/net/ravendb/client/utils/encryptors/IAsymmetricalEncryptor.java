package net.ravendb.client.utils.encryptors;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;


public interface IAsymmetricalEncryptor {
  public byte[] encrypt(byte[] bytes) throws InvalidKeyException,  NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException;

  public void importParameters(byte[] exponent, byte[] modulus) throws NoSuchAlgorithmException, InvalidKeySpecException;
}
