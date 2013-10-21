package net.ravendb.client.connection.profiling;

/**
 * The status of the request
 */
public enum RequestStatus {

  /**
   * The request was sent to the server
   */
  SEND_TO_SERVER,
  /**
   * The request was served directly from the local cache
   * after checking with the server to see if it was still
   * up to date
   */
  CACHED,
  /**
   * The request was served from the local cache without
   * checking with the server and may be out of date
   */
  AGGRESSIVELY_CACHED,
  /**
   * The server returned an error
   */
  ERROR_ON_SERVER;

}
