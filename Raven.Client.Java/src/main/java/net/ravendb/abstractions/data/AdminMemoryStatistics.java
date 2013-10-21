package net.ravendb.abstractions.data;


public class AdminMemoryStatistics {
  private double databaseCacheSizeInMB;
  private double managedMemorySizeInMB;
  private double totalProcessMemorySizeInMB;

  public double getDatabaseCacheSizeInMB() {
    return databaseCacheSizeInMB;
  }

  public void setDatabaseCacheSizeInMB(double databaseCacheSizeInMB) {
    this.databaseCacheSizeInMB = databaseCacheSizeInMB;
  }

  public double getManagedMemorySizeInMB() {
    return managedMemorySizeInMB;
  }

  public void setManagedMemorySizeInMB(double managedMemorySizeInMB) {
    this.managedMemorySizeInMB = managedMemorySizeInMB;
  }

  public double getTotalProcessMemorySizeInMB() {
    return totalProcessMemorySizeInMB;
  }

  public void setTotalProcessMemorySizeInMB(double totalProcessMemorySizeInMB) {
    this.totalProcessMemorySizeInMB = totalProcessMemorySizeInMB;
  }

}
