package net.ravendb.client.utils.encryptors;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.RSAPublicKeySpec;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;


public class FipsAsymmetricalEncryptor implements IAsymmetricalEncryptor {

  private Key publicKey;

  @Override
  public byte[] encrypt(byte[] bytes) throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException, IllegalBlockSizeException, BadPaddingException {
    Cipher cipher = Cipher.getInstance("RSA/None/OAEPWITHSHA-1ANDMGF1PADDING");
    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
    return cipher.doFinal(bytes);
  }

  @Override
  public void importParameters(byte[] exponent, byte[] modulus) throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeySpec keySpec = new RSAPublicKeySpec(new BigInteger(1, modulus), new BigInteger(1, exponent));
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    publicKey = keyFactory.generatePublic(keySpec);
  }

}
