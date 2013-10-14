package net.ravendb.abstractions.commands;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.json.linq.RavenJObject;

/**
 * A single operation inside the batch.
 *
 */
public interface ICommandData {

  /**
   * Gets the key
   * @return
   */
  public String getKey();

  /**
   * Gets the method
   * @return
   */
  public HttpMethods getMethod();

  /**
   * Gets the Etag.
   * @return
   */
  public Etag getEtag();

  /**
   * Gets the metadata.
   * @return
   */
  public RavenJObject getMetadata();

  /**
   * Gets the additional data.
   * @return
   */
  public RavenJObject getAdditionalData();

  /**
   * Sets the additional metadata
   * @param additionalMeta
   */
  public void setAdditionalData(RavenJObject additionalMeta);

  /**
   * Translate the instance to a Json object.
   * @return
   */
  public RavenJObject toJson();

}
