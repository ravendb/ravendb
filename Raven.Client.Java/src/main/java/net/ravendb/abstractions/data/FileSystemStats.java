package net.ravendb.abstractions.data;

import java.util.List;


public class FileSystemStats {
  private String name;
  private Long fileCount;
  private FileSystemMetrics metrics;
  private List<SynchronizationDetails> activeSyncs;
  private List<SynchronizationDetails> pendingSyncs;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Long getFileCount() {
    return fileCount;
  }

  public void setFileCount(Long fileCount) {
    this.fileCount = fileCount;
  }

  public FileSystemMetrics getMetrics() {
    return metrics;
  }

  public void setMetrics(FileSystemMetrics metrics) {
    this.metrics = metrics;
  }

  public List<SynchronizationDetails> getActiveSyncs() {
    return activeSyncs;
  }

  public void setActiveSyncs(List<SynchronizationDetails> activeSyncs) {
    this.activeSyncs = activeSyncs;
  }

  public List<SynchronizationDetails> getPendingSyncs() {
    return pendingSyncs;
  }

  public void setPendingSyncs(List<SynchronizationDetails> pendingSyncs) {
    this.pendingSyncs = pendingSyncs;
  }

}
