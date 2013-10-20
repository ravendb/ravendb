package net.ravendb.client.utils.encryptors;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class FipsEncryptor implements IEncryptor {

  public static class FipsHashEncryptor extends HashEncryptorBase implements IHashEncryptor {
    @Override
    public byte[] compute20(byte[] bytes) {
      try {
        return computeHash("SHA-1", bytes, null);
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
