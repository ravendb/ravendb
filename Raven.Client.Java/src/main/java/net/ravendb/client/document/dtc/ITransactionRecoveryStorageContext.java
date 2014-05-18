package net.ravendb.client.document.dtc;

import java.io.InputStream;
import java.util.List;

import net.ravendb.abstractions.closure.Action1;


public interface ITransactionRecoveryStorageContext {
  public void createFile(String name, Action1<InputStream> createFile);
  public void deleteFile(String name);
  public List<String> getFileNames(String filter);
  public InputStream openRead(String name);
}
