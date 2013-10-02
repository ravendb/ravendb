package raven.client.utils.encryptors;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.Mac;


public class FipsEncryptor implements IEncryptor {

  public static class FipsHashEncryptor extends HashEncryptorBase implements IHashEncryptor {
    @Override
    public byte[] compute20(byte[] bytes) {
      try {
        Mac mac = Mac.getInstance("HmacSHA1"); //TODO: initialize with key
        byte[] digest = mac.doFinal(bytes);
        return digest;
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public IHashEncryptor createHash() {
    return new FipsHashEncryptor();
  }

  @Override
  public ISymmetricalEncryptor createSymmetrical(int keySize) {
    return new FipsSymmetricalEncryptor();
  }

  @Override
  public IAsymmetricalEncryptor createAsymmetrical(byte[] exponent, byte[] modulus) throws NoSuchAlgorithmException, InvalidKeySpecException {
    FipsAsymmetricalEncryptor asymmetrical = new FipsAsymmetricalEncryptor();
    asymmetrical.importParameters(exponent, modulus);
    return asymmetrical;
  }

}
