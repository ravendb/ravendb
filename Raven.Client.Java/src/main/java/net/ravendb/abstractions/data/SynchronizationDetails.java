package net.ravendb.abstractions.data;

import java.util.UUID;


public class SynchronizationDetails {
  private String fileName;
  private UUID fileETag;
  private String destinationUrl;
  private SynchronizationType type;

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public UUID getFileETag() {
    return fileETag;
  }

  public void setFileETag(UUID fileETag) {
    this.fileETag = fileETag;
  }

  public String getDestinationUrl() {
    return destinationUrl;
  }

  public void setDestinationUrl(String destinationUrl) {
    this.destinationUrl = destinationUrl;
  }

  public SynchronizationType getType() {
    return type;
  }

  public void setType(SynchronizationType type) {
    this.type = type;
  }

}
