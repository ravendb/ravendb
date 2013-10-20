package net.ravendb.client.utils.encryptors;


public interface IHashEncryptor {
  public byte[] compute20(byte[] bytes);
}
