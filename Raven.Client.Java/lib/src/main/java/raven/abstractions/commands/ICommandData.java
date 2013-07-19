package raven.abstractions.commands;

import raven.abstractions.data.Etag;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.TransactionInformation;
import raven.abstractions.json.linq.RavenJObject;

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
   * Sets the transactionInformation
   * @param transactionInformation
   */
  public void setTransactionInformation(TransactionInformation transactionInformation);

  /**
   * Gets the transactionInformation
   * @return
   */
  public TransactionInformation getTransactionInformation();

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
