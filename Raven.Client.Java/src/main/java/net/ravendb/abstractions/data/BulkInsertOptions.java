package net.ravendb.abstractions.data;

public class BulkInsertOptions {
  private boolean overwriteExisting;
  private boolean checkReferencesInIndexes;
  private int batchSize;

  public BulkInsertOptions() {
    batchSize = 512;
  }

  public boolean isOverwriteExisting() {
    return overwriteExisting;
  }

  public void setOverwriteExisting(boolean overwriteExisting) {
    this.overwriteExisting = overwriteExisting;
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
