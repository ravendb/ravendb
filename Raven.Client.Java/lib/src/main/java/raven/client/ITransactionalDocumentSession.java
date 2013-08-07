package raven.client;

import java.util.UUID;

/**
 * Implementers of this interface provide transactional operations
 * Note that this interface is mostly useful only for expert usage
 */
public interface ITransactionalDocumentSession {

  /**
   * The transaction resource manager identifier
   * @return
   */
  public UUID getResourceManagerId();

  /**
   * The db name for this session
   * @return
   */
  public String getDatabaseName();

  /**
   * Commits the transaction specified.
   * @param txId
   */
  public void commit(String txId);

  /**
   * Rollbacks the transaction specified.
   * @param txId
   */
  public void rollback(String txId);

  /**
   * Prepares the transaction on the server.
   * @param txId
   */
  public void prepareTransaction(String txId);
}
