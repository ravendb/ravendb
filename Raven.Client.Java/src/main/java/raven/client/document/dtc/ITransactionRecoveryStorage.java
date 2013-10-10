package raven.client.document.dtc;

public interface ITransactionRecoveryStorage {
  public ITransactionRecoveryStorageContext create();
}
