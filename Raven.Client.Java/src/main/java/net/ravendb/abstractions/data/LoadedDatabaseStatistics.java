package net.ravendb.abstractions.data;

import java.util.Date;


public class LoadedDatabaseStatistics {
  private String name;
  private Date lastActivity;
  private long transactionalStorageSize;
  private String transactionalStorageSizeHumaneSize;
  private long indexStorageSize;
  private String indexStorageHumaneSize;
  private long totalDatabaseSize;
  private String totalDatabaseHumaneSize;
  private long countOfDocuments;
  private double requestsPerSecond;
  private int concurrentRequests;
  private double databaseTransactionVersionSizeInMB;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Date getLastActivity() {
    return lastActivity;
  }

  public void setLastActivity(Date lastActivity) {
    this.lastActivity = lastActivity;
  }

  public long getTransactionalStorageSize() {
    return transactionalStorageSize;
  }

  public void setTransactionalStorageSize(long transactionalStorageSize) {
    this.transactionalStorageSize = transactionalStorageSize;
  }

  public String getTransactionalStorageSizeHumaneSize() {
    return transactionalStorageSizeHumaneSize;
  }

  public void setTransactionalStorageSizeHumaneSize(String transactionalStorageSizeHumaneSize) {
    this.transactionalStorageSizeHumaneSize = transactionalStorageSizeHumaneSize;
  }

  public long getIndexStorageSize() {
    return indexStorageSize;
  }

  public void setIndexStorageSize(long indexStorageSize) {
    this.indexStorageSize = indexStorageSize;
  }

  public String getIndexStorageHumaneSize() {
    return indexStorageHumaneSize;
  }

  public void setIndexStorageHumaneSize(String indexStorageHumaneSize) {
    this.indexStorageHumaneSize = indexStorageHumaneSize;
  }

  public long getTotalDatabaseSize() {
    return totalDatabaseSize;
  }

  public void setTotalDatabaseSize(long totalDatabaseSize) {
    this.totalDatabaseSize = totalDatabaseSize;
  }

  public String getTotalDatabaseHumaneSize() {
    return totalDatabaseHumaneSize;
  }

  public void setTotalDatabaseHumaneSize(String totalDatabaseHumaneSize) {
    this.totalDatabaseHumaneSize = totalDatabaseHumaneSize;
  }

  public long getCountOfDocuments() {
    return countOfDocuments;
  }

  public void setCountOfDocuments(long countOfDocuments) {
    this.countOfDocuments = countOfDocuments;
  }

  public double getRequestsPerSecond() {
    return requestsPerSecond;
  }

  public void setRequestsPerSecond(double requestsPerSecond) {
    this.requestsPerSecond = requestsPerSecond;
  }

  public int getConcurrentRequests() {
    return concurrentRequests;
  }

  public void setConcurrentRequests(int concurrentRequests) {
    this.concurrentRequests = concurrentRequests;
  }

  public double getDatabaseTransactionVersionSizeInMB() {
    return databaseTransactionVersionSizeInMB;
  }

  public void setDatabaseTransactionVersionSizeInMB(double databaseTransactionVersionSizeInMB) {
    this.databaseTransactionVersionSizeInMB = databaseTransactionVersionSizeInMB;
  }


}
