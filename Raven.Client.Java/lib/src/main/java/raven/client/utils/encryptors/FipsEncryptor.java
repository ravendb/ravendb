package raven.client.utils.encryptors;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;


public class FipsEncryptor implements IEncryptor {

  @Override
  public IHashEncryptor createHash() {
    // TODO Auto-generated method stub
    return null;
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
