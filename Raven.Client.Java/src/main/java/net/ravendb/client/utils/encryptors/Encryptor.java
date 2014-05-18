package net.ravendb.client.utils.encryptors;


public class Encryptor {
  private static IEncryptor current = new DefaultEncryptor();

  public static IEncryptor getCurrent() {
    return current;
  }

  public static void initialize(boolean useFips) {
    current = useFips ? new FipsEncryptor() : new DefaultEncryptor();
  }
}
