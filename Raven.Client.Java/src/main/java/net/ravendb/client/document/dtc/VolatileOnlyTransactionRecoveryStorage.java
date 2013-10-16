package net.ravendb.client.document.dtc;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import net.ravendb.abstractions.closure.Action1;


public class VolatileOnlyTransactionRecoveryStorage implements ITransactionRecoveryStorageContext, ITransactionRecoveryStorage {

  @Override
  public void createFile(String name, Action1<InputStream> createFile) {
  }

  @Override
  public void deleteFile(String name) {
  }

  @Override
  public List<String> getFileNames(String filter) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public InputStream openRead(String name) {
    throw new IllegalStateException("not supported");
  }

  @Override
  public ITransactionRecoveryStorageContext create() {
    return this;
  }
}
