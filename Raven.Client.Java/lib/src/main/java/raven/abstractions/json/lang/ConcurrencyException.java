package raven.abstractions.json.lang;

import java.util.UUID;

public class ConcurrencyException extends RuntimeException {

  private UUID expectedEtag;
  private UUID actualEtag;

  /**
   * @return the expectedEtag
   */
  public UUID getExpectedEtag() {
    return expectedEtag;
  }

  /**
   * @return the actualEtag
   */
  public UUID getActualEtag() {
    return actualEtag;
  }

  public ConcurrencyException(UUID expectedEtag, UUID actualEtag, Throwable cause) {
    super("Expected Etag:" + expectedEtag + ", actual:" + actualEtag + cause.getMessage(), cause);
    this.expectedEtag = expectedEtag;
    this.actualEtag = actualEtag;
  }

}
