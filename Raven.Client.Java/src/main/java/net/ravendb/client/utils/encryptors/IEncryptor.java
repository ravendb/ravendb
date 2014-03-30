package net.ravendb.client.utils.encryptors;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;


public interface IEncryptor {
  public IHashEncryptor createHash();

  public ISymmetricalEncryptor createSymmetrical(int keySize);

  public IAsymmetricalEncryptor createAsymmetrical(byte[] exponent, byte[] modulus) throws NoSuchAlgorithmException, InvalidKeySpecException;

}
