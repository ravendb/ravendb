package net.ravendb.client.exceptions;

import net.ravendb.abstractions.data.Etag;

public class ConflictException extends RuntimeException {
  private String[] conflictedVersionIds;
  private Etag etag;
  /**
   * @return the conflictedVersionIds
   */
  public String[] getConflictedVersionIds() {
    return conflictedVersionIds;
  }
  /**
   * @param conflictedVersionIds the conflictedVersionIds to set
   */
  public void setConflictedVersionIds(String[] conflictedVersionIds) {
    this.conflictedVersionIds = conflictedVersionIds;
  }
  /**
   * @return the etag
   */
  public Etag getEtag() {
    return etag;
  }
  /**
   * @param etag the etag to set
   */
  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  public ConflictException(boolean properlyHandlesClientSideResolution) {
    // empty by design
  }

  public ConflictException(String message, boolean properlyHandlesClientSideResolution) {
    super(message);
  }

  public ConflictException(String message, Exception inner, boolean properlyHandlesClientSideResolution) {
    super(message, inner);
  }
}
