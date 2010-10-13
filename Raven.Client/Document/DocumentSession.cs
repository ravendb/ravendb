using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using Newtonsoft.Json.Linq;
using System;
using Raven.Client.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Document
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentSession, ITransactionalDocumentSession, ISyncAdvancedSessionOperation
    {
        /// <summary>
        /// Gets the database commands.
        /// </summary>
        /// <value>The database commands.</value>
        public IDatabaseCommands DatabaseCommands { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentSession"/> class.
        /// </summary>
        /// <param name="documentStore">The document store.</param>
        /// <param name="storeListeners">The store listeners.</param>
        /// <param name="deleteListeners">The delete listeners.</param>
        public DocumentSession(DocumentStore documentStore, IDocumentStoreListener[] storeListeners, IDocumentDeleteListener[] deleteListeners)
            : base(documentStore, storeListeners, deleteListeners)
        {
            DatabaseCommands = documentStore.DatabaseCommands;
        }

        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        public ISyncAdvancedSessionOperation Advanced
        {
            get { return this; }
        }

        /// <summary>
        /// Loads the specified entity with the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public T Load<T>(string id)
        {
            object existingEntity;
            if (entitiesByKey.TryGetValue(id, out existingEntity))
            {
                return (T)existingEntity;
            }

            IncrementRequestCount();
            var sp = Stopwatch.StartNew();
            JsonDocument documentFound;
            do
            {
                try
                {
                    Trace.WriteLine(string.Format("Loading document [{0}] from {1}", id, StoreIdentifier));
                    documentFound = DatabaseCommands.Get(id);
                }
                catch (WebException ex)
                {
                    var httpWebResponse = ex.Response as HttpWebResponse;
                    if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
                        return default(T);
                    throw;
                }
                if (documentFound == null)
                    return default(T);

            } while (
                documentFound.NonAuthoritiveInformation &&
                AllowNonAuthoritiveInformation == false &&
                sp.Elapsed < NonAuthoritiveInformationTimeout
                );


            return TrackEntity<T>(documentFound);
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public T[] Load<T>(params string[] ids)
        {
            return LoadInternal<T>(ids, null);
        }


        internal T[] LoadInternal<T>(string[] ids, string[] includes)
        {
            IncrementRequestCount();
            Trace.WriteLine(string.Format("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), StoreIdentifier));
            MultiLoadResult multiLoadResult;
            JsonDocument[] includeResults;
            JsonDocument[] results;
            var sp = Stopwatch.StartNew();
            do
            {
                multiLoadResult = documentStore.DatabaseCommands.Get(ids, includes);
                includeResults = SerializationHelper.JObjectsToJsonDocuments(multiLoadResult.Includes).ToArray();
                results = SerializationHelper.JObjectsToJsonDocuments(multiLoadResult.Results).ToArray();
            } while (
                AllowNonAuthoritiveInformation == false &&
                results.Any(x => x.NonAuthoritiveInformation) &&
                sp.Elapsed < NonAuthoritiveInformationTimeout
                );

            foreach (var include in includeResults)
            {
                TrackEntity<object>(include);
            }

            return results
                .Select(TrackEntity<T>)
                .ToArray();
        }

        /// <summary>
        /// Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <returns></returns>
        public IRavenQueryable<T> Query<T>(string indexName)
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            return new RavenQueryable<T>(new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics),ravenQueryStatistics);
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return Query<T>(indexCreator.IndexName);
        }

        /// <summary>
        /// Refreshes the specified entity from Raven server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Refresh<T>(T entity)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException("Cannot refresh a trasient instance");
            var jsonDocument = documentStore.DatabaseCommands.Get(value.Key);
            if (jsonDocument == null)
                throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

            value.Metadata = jsonDocument.Metadata;
            value.OriginalMetadata = new JObject(jsonDocument.Metadata);
            value.ETag = jsonDocument.Etag;
            value.OriginalValue = jsonDocument.DataAsJson;
            var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
            foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(entity))
            {
                property.SetValue(entity, property.GetValue(newEntity));
            }
        }

        /// <summary>
        /// Get the json document by key from the store
        /// </summary>
        protected override JsonDocument GetJsonDocument(string documentKey)
        {
             var jsonDocument = documentStore.DatabaseCommands.Get(documentKey);
            if (jsonDocument == null)
                throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
            return jsonDocument;
        }

        /// <summary>
        /// Begin a load while including the specified path
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public ILoaderWithInclude<object> Include(string path)
        {
            return new MultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Gets the document URL for the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public string GetDocumentUrl(object entity)
        {
            if (string.IsNullOrEmpty(documentStore.Url))
                throw new InvalidOperationException("Could not provide document url for embedded instance");

            DocumentMetadata value;
            string baseUrl = documentStore.Url.EndsWith("/") ? documentStore.Url + "docs/" : documentStore.Url + "/docs/";
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
            {
                return baseUrl + GetOrGenerateDocumentKey(entity);
            }

            return baseUrl + value.Key;
        }

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        public void SaveChanges()
        {
            var data = PrepareForSaveChanges();
            if (data.Commands.Count == 0)
                return; // nothing to do here
            IncrementRequestCount();
            Trace.WriteLine(string.Format("Saving {0} changes to {1}", data.Commands.Count, StoreIdentifier));
            UpdateBatchResults(DatabaseCommands.Batch(data.Commands.ToArray()), data.Entities);
        }


        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var index = new TIndexCreator();
            return LuceneQuery<T>(index.IndexName);
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <returns></returns>
        public IDocumentQuery<T> LuceneQuery<T>(string indexName)
        {
            return new DocumentQuery<T>(this, DatabaseCommands, indexName, null);
        }

        /// <summary>
        /// Commits the specified tx id.
        /// </summary>
        /// <param name="txId">The tx id.</param>
        public override void Commit(Guid txId)
        {
            IncrementRequestCount();
            documentStore.DatabaseCommands.Commit(txId);
            ClearEnlistment();
        }

        /// <summary>
        /// Rollbacks the specified tx id.
        /// </summary>
        /// <param name="txId">The tx id.</param>
        public override void Rollback(Guid txId)
        {
            IncrementRequestCount();
            documentStore.DatabaseCommands.Rollback(txId);
            ClearEnlistment();
        }

        /// <summary>
        /// Promotes the transaction.
        /// </summary>
        /// <param name="fromTxId">From tx id.</param>
        /// <returns></returns>
        public override byte[] PromoteTransaction(Guid fromTxId)
        {
            return documentStore.DatabaseCommands.PromoteTransaction(fromTxId);
        }

        /// <summary>
        /// Stores the recovery information for the specified transaction
        /// </summary>
        /// <param name="resourceManagerId"></param>
        /// <param name="txId">The tx id.</param>
        /// <param name="recoveryInformation">The recovery information.</param>
        public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
        {
            documentStore.DatabaseCommands.StoreRecoveryInformation(resourceManagerId, txId, recoveryInformation);
        }

        /// <summary>
        /// Dynamically queries RavenDB using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        public IRavenQueryable<T> Query<T>()
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            return new RavenQueryable<T>(new DynamicRavenQueryProvider<T>(this, ravenQueryStatistics), ravenQueryStatistics);
        }

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        public IDocumentQuery<T> DynamicLuceneQuery<T>()
        {
            string indexName = "dynamic";
            if (typeof(T) != typeof(object))
            {
                indexName += "/" + Conventions.GetTypeTagName(typeof(T));
            }
            return LuceneQuery<T>(indexName);
        }

        /// <summary>
        /// Metadata held about an entity by the session
        /// </summary>
        public class DocumentMetadata
        {
            /// <summary>
            /// Gets or sets the original value.
            /// </summary>
            /// <value>The original value.</value>
            public JObject OriginalValue { get; set; }
            /// <summary>
            /// Gets or sets the metadata.
            /// </summary>
            /// <value>The metadata.</value>
            public JObject Metadata { get; set; }
            /// <summary>
            /// Gets or sets the ETag.
            /// </summary>
            /// <value>The ETag.</value>
            public Guid? ETag { get; set; }
            /// <summary>
            /// Gets or sets the key.
            /// </summary>
            /// <value>The key.</value>
            public string Key { get; set; }
            /// <summary>
            /// Gets or sets the original metadata.
            /// </summary>
            /// <value>The original metadata.</value>
            public JObject OriginalMetadata { get; set; }
        }

        /// <summary>
        /// Data for a batch command to the server
        /// </summary>
        public class SaveChangesData
        {
            /// <summary>
            /// Gets or sets the commands.
            /// </summary>
            /// <value>The commands.</value>
            public IList<ICommandData> Commands { get; set; }
            /// <summary>
            /// Gets or sets the entities.
            /// </summary>
            /// <value>The entities.</value>
            public IList<object> Entities { get; set; }
        }
    }
}
