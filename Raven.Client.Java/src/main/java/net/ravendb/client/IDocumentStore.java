package net.ravendb.client;

import java.util.Map;

import net.ravendb.abstractions.data.BulkInsertOptions;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.connection.implementation.HttpJsonRequestFactory;
import net.ravendb.client.document.BulkInsertOperation;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.document.OpenSessionOptions;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;


/**
 * Interface for managing access to RavenDB and open sessions.
 */
public interface IDocumentStore extends IDisposalNotification {

  /**
   * Subscribe to change notifications from the server
   * @return
   */
  public IDatabaseChanges changes();

  /**
   * Subscribe to change notifications from the server
   * @param database
   * @return
   */
  public IDatabaseChanges changes(String database);

  /**
   * Setup the context for aggressive caching.
   *
   * Aggressive caching means that we will not check the server to see whatever the response
   * we provide is current or not, but will serve the information directly from the local cache
   * without touching the server.
   * @param cacheDurationInMilis
   * @return
   */
  public AutoCloseable aggressivelyCacheFor(long cacheDurationInMilis);

  /**
   * Setup the context for aggressive caching.
   *
   * Aggressive caching means that we will not check the server to see whatever the response
   * we provide is current or not, but will serve the information directly from the local cache
   * without touching the server.
   * @return
   */
  public AutoCloseable aggressivelyCache();

  /**
   * Setup the context for no aggressive caching
   *
   * This is mainly useful for internal use inside RavenDB, when we are executing
   * queries that has been marked with WaitForNonStaleResults, we temporarily disable
   * aggressive caching.
   * @return
   */
  public AutoCloseable disableAggressiveCaching();

  /**
   * Setup the WebRequest timeout for the session
   * @param timeout Specify the timeout duration
   * @return Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
   */
  public AutoCloseable setRequestsTimeoutFor(long timeout);

  /**
   * Gets the shared operations headers.
   * @return
   */
  public Map<String, String> getSharedOperationsHeaders();


  /**
   * Get the {@link HttpJsonRequestFactory} for this store
   * @return
   */
  public HttpJsonRequestFactory getJsonRequestFactory();

  /**
   * Sets the identifier
   * @param identifier
   */
  public void setIdentifier(String identifier);

  /**
   * Gets the identifier
   * @return
   */
  public String getIdentifier();

  /**
   * Initializes this instance.
   * @return
   */
  public IDocumentStore initialize();

  /**
   * Opens the session.
   * @return
   */
  public IDocumentSession openSession();

  /**
   * Opens the session for a particular database
   * @param database
   * @return
   */
  public IDocumentSession openSession(String database);

  /**
   * Opens the session with the specified options.
   * @param sessionOptions
   * @return
   */
  public IDocumentSession openSession(OpenSessionOptions sessionOptions);

  /**
   * Gets the database commands.
   * @return
   */
  public net.ravendb.client.connection.IDatabaseCommands getDatabaseCommands();

  /**
   * Executes the index creation.
   * @param indexCreationTask
   */
  public void executeIndex(AbstractIndexCreationTask indexCreationTask);

  /**
   * executes the transformer creation
   * @param transformerCreationTask
   */
  public void executeTransformer(AbstractTransformerCreationTask transformerCreationTask);

  /**
   * Gets the conventions.
   * @return
   */
  public DocumentConvention getConventions();

  /**
   * Gets the URL.
   * @return
   */
  public String getUrl();

  /**
   * Gets the etag of the last document written by any session belonging to this
   * document store
   * @return
   */
  public Etag getLastWrittenEtag();

  /**
   * Performs bulk insert
   * @return
   */
  public BulkInsertOperation bulkInsert();

  /**
   * Performs bulk insert
   * @param database
   * @return
   */
  public BulkInsertOperation bulkInsert(String database);

  /**
   * Performs bulk insert
   * @param database
   * @param options
   * @return
   */
  public BulkInsertOperation bulkInsert(String database, BulkInsertOptions options);

}
