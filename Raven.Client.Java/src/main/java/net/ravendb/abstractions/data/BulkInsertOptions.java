package net.ravendb.abstractions.data;

public class BulkInsertOptions {
  private boolean checkForUpdates;
  private boolean checkReferencesInIndexes;
  private int batchSize;

  public BulkInsertOptions() {
    batchSize = 512;
  }

  public boolean isCheckForUpdates() {
    return checkForUpdates;
  }
  public void setCheckForUpdates(boolean checkForUpdates) {
    this.checkForUpdates = checkForUpdates;
  }
  public boolean isCheckReferencesInIndexes() {
    return checkReferencesInIndexes;
  }
  public void setCheckReferencesInIndexes(boolean checkReferencesInIndexes) {
    this.checkReferencesInIndexes = checkReferencesInIndexes;
  }
  public int getBatchSize() {
    return batchSize;
  }
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }


}
