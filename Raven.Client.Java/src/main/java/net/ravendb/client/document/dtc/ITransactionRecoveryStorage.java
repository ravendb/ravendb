package net.ravendb.client.document.dtc;

public interface ITransactionRecoveryStorage {
  public ITransactionRecoveryStorageContext create();
}
