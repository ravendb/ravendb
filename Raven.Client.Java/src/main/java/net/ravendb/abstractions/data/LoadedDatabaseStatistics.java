package net.ravendb.abstractions.data;

import java.util.Date;
import java.util.List;


public class LoadedDatabaseStatistics {
  private String name;
  private Date lastActivity;

  private long transactionalStorageAllocatedSize;
  private String transactionalStorageAllocatedSizeHumaneSize;
  private long transactionalStorageUsedSize;
  private String transactionalStorageUsedSizeHumaneSize;

  private long indexStorageSize;
  private String indexStorageHumaneSize;
  private long totalDatabaseSize;
  private String totalDatabaseHumaneSize;
  private long countOfDocuments;
  private long countOfAttachments;
  private double databaseTransactionVersionSizeInMB;
  private List<DatabaseMetrics> metrics;

  public List<DatabaseMetrics> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<DatabaseMetrics> metrics) {
    this.metrics = metrics;
  }

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

  public long getCountOfAttachments() {
    return countOfAttachments;
  }

  public void setCountOfAttachments(long countOfAttachments) {
    this.countOfAttachments = countOfAttachments;
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

  public double getDatabaseTransactionVersionSizeInMB() {
    return databaseTransactionVersionSizeInMB;
  }

  public void setDatabaseTransactionVersionSizeInMB(double databaseTransactionVersionSizeInMB) {
    this.databaseTransactionVersionSizeInMB = databaseTransactionVersionSizeInMB;
  }


  public long getTransactionalStorageAllocatedSize() {
    return transactionalStorageAllocatedSize;
  }


  public void setTransactionalStorageAllocatedSize(long transactionalStorageAllocatedSize) {
    this.transactionalStorageAllocatedSize = transactionalStorageAllocatedSize;
  }


  public String getTransactionalStorageAllocatedSizeHumaneSize() {
    return transactionalStorageAllocatedSizeHumaneSize;
  }


  public void setTransactionalStorageAllocatedSizeHumaneSize(String transactionalStorageAllocatedSizeHumaneSize) {
    this.transactionalStorageAllocatedSizeHumaneSize = transactionalStorageAllocatedSizeHumaneSize;
  }


  public long getTransactionalStorageUsedSize() {
    return transactionalStorageUsedSize;
  }


  public void setTransactionalStorageUsedSize(long transactionalStorageUsedSize) {
    this.transactionalStorageUsedSize = transactionalStorageUsedSize;
  }


  public String getTransactionalStorageUsedSizeHumaneSize() {
    return transactionalStorageUsedSizeHumaneSize;
  }


  public void setTransactionalStorageUsedSizeHumaneSize(String transactionalStorageUsedSizeHumaneSize) {
    this.transactionalStorageUsedSizeHumaneSize = transactionalStorageUsedSizeHumaneSize;
  }

}
