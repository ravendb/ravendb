//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System;
using Raven.Abstractions.Data;
#if !NET_3_5
using Raven.Client.Connection.Async;
#endif
using Raven.Client.Connection;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
#if !SILVERLIGHT
	/// <summary>
	/// Implements Unit of Work for accessing the RavenDB server
	/// </summary>
	public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentSession, ITransactionalDocumentSession, ISyncAdvancedSessionOperation, IDocumentQueryGenerator
	{
#if !NET_3_5
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
#endif

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		public IDatabaseCommands DatabaseCommands { get; private set; }

#if !NET_3_5
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return asyncDatabaseCommands; }
		}
#endif

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentSession"/> class.
		/// </summary>
		public DocumentSession(DocumentStore documentStore, 
			IDocumentQueryListener[] queryListeners,
			IDocumentStoreListener[] storeListeners, 
			IDocumentDeleteListener[] deleteListeners, 
			IDatabaseCommands databaseCommands
#if !NET_3_5
			, IAsyncDatabaseCommands asyncDatabaseCommands
#endif
			)
			: base(documentStore, queryListeners, storeListeners, deleteListeners)
		{
#if !NET_3_5
			this.asyncDatabaseCommands = asyncDatabaseCommands;
#endif
			DatabaseCommands = databaseCommands;
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
#if !SILVERLIGHT
			var sp = Stopwatch.StartNew();
#else
			var startTime = DateTime.Now;
#endif
			JsonDocument documentFound;
			do
			{
				try
				{
					Debug.WriteLine(string.Format("Loading document [{0}] from {1}", id, StoreIdentifier));
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
#if !SILVERLIGHT
				sp.Elapsed < NonAuthoritiveInformationTimeout
#else
				(DateTime.Now - startTime) < NonAuthoritiveInformationTimeout
#endif
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

		/// <summary>
		/// Loads the specified entities with the specified ids.
		/// </summary>
		/// <param name="ids">The ids.</param>
		public T[] Load<T>(IEnumerable<string> ids)
		{
			return LoadInternal<T>(ids.ToArray(), null);
		}

		/// <summary>
		/// Loads the specified entities with the specified id after applying
		/// conventions on the provided id to get the real document id.
		/// </summary>
		/// <remarks>
		/// This method allows you to call:
		/// Load{Post}(1)
		/// And that call will internally be translated to 
		/// Load{Post}("posts/1");
		/// 
		/// Or whatever your conventions specify.
		/// </remarks>
		public T Load<T>(ValueType id)
		{
			var documentKey = Conventions.FindFullDocumentKeyFromValueTypeIdentifier(id, typeof(T));
			return Load<T>(documentKey);
		}

		internal T[] LoadInternal<T>(string[] ids, string[] includes)
		{
			if(ids.Length == 0)
				return new T[0];

			IncrementRequestCount();
			Debug.WriteLine(string.Format("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), StoreIdentifier));
			MultiLoadResult multiLoadResult;
			JsonDocument[] includeResults;
			JsonDocument[] results;
#if !SILVERLIGHT
			var sp = Stopwatch.StartNew();
#else
			var startTime = DateTime.Now;
#endif
			do
			{

				multiLoadResult = DatabaseCommands.Get(ids, includes);
				includeResults = SerializationHelper.RavenJObjectsToJsonDocuments(multiLoadResult.Includes).ToArray();
				results = SerializationHelper.RavenJObjectsToJsonDocuments(multiLoadResult.Results).ToArray();
			} while (
				AllowNonAuthoritiveInformation == false &&
				results.Any(x => x.NonAuthoritiveInformation) &&
#if !SILVERLIGHT
				sp.Elapsed < NonAuthoritiveInformationTimeout
#else 
				(DateTime.Now - startTime) < NonAuthoritiveInformationTimeout
#endif
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
			return new RavenQueryInspector<T>(new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, DatabaseCommands
#if !NET_3_5
				, AsyncDatabaseCommands
#endif
				),ravenQueryStatistics, indexName, null,  DatabaseCommands
#if !NET_3_5
				, AsyncDatabaseCommands
#endif
					);
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
			IncrementRequestCount();
			var jsonDocument = DatabaseCommands.Get(value.Key);
			if (jsonDocument == null)
				throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

			value.Metadata = jsonDocument.Metadata;
			value.OriginalMetadata = (RavenJObject)jsonDocument.Metadata.CloneToken();
			value.ETag = jsonDocument.Etag;
			value.OriginalValue = jsonDocument.DataAsJson;
			var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
			foreach (PropertyInfo property in entity.GetType().GetProperties())
			{
				if (!property.CanWrite || !property.CanRead || property.GetIndexParameters().Length != 0) 
					continue;
				property.SetValue(entity, property.GetValue(newEntity, null), null);
			}
		}

		/// <summary>
		/// Get the json document by key from the store
		/// </summary>
		protected override JsonDocument GetJsonDocument(string documentKey)
		{
			 var jsonDocument = DatabaseCommands.Get(documentKey);
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
				string id;
				TryGetIdFromInstance(entity, out id);
				if(string.IsNullOrEmpty(id))
					throw new InvalidOperationException("Could not figure out identifier for transient instance");
				return baseUrl + id;
			}

			return baseUrl + value.Key;
		}

		/// <summary>
		/// Saves all the changes to the Raven server.
		/// </summary>
		public void SaveChanges()
		{
		    using(EntitiesToJsonCachingScope())
		    {
                var data = PrepareForSaveChanges();
                if (data.Commands.Count == 0)
                    return; // nothing to do here
                IncrementRequestCount();
                Debug.WriteLine(string.Format("Saving {0} changes to {1}", data.Commands.Count, StoreIdentifier));
                UpdateBatchResults(DatabaseCommands.Batch(data.Commands), data.Entities);
            }
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
#if !NET_3_5
			return new DocumentQuery<T>(this, DatabaseCommands, null, indexName, null, queryListeners);
#else
			return new DocumentQuery<T>(this, DatabaseCommands, indexName, null, queryListeners);
#endif
		}

		/// <summary>
		/// Commits the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public override void Commit(Guid txId)
		{
			IncrementRequestCount();
			DatabaseCommands.Commit(txId);
			ClearEnlistment();
		}

		/// <summary>
		/// Rollbacks the specified tx id.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		public override void Rollback(Guid txId)
		{
			IncrementRequestCount();
			DatabaseCommands.Rollback(txId);
			ClearEnlistment();
		}

		/// <summary>
		/// Promotes the transaction.
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns></returns>
		public override byte[] PromoteTransaction(Guid fromTxId)
		{
			IncrementRequestCount();
			return DatabaseCommands.PromoteTransaction(fromTxId);
		}

		/// <summary>
		/// Stores the recovery information for the specified transaction
		/// </summary>
		/// <param name="resourceManagerId"></param>
		/// <param name="txId">The tx id.</param>
		/// <param name="recoveryInformation">The recovery information.</param>
		public void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation)
		{
			IncrementRequestCount();
			DatabaseCommands.StoreRecoveryInformation(resourceManagerId, txId, recoveryInformation);
		}

		/// <summary>
		/// Dynamically queries RavenDB using LINQ
		/// </summary>
		/// <typeparam name="T">The result of the query</typeparam>
		public IRavenQueryable<T> Query<T>()
		{
			string indexName = "dynamic";
			if (typeof(T) != typeof(object))
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			var ravenQueryStatistics = new RavenQueryStatistics();
			return new RavenQueryInspector<T>(
				new DynamicRavenQueryProvider<T>(this, indexName, ravenQueryStatistics, Advanced.DatabaseCommands
#if !NET_3_5
					, Advanced.AsyncDatabaseCommands
#endif
				), 
				ravenQueryStatistics,
				indexName,
				null,
				Advanced.DatabaseCommands
#if !NET_3_5
				, Advanced.AsyncDatabaseCommands
#endif
			);
		}

		/// <summary>
		/// Dynamically query RavenDB using Lucene syntax
		/// </summary>
		public IDocumentQuery<T> LuceneQuery<T>()
		{
			string indexName = "dynamic";
			if (typeof(T) != typeof(object))
			{
				indexName += "/" + Conventions.GetTypeTagName(typeof(T));
			}
			return LuceneQuery<T>(indexName);
		}

		/// <summary>
		/// Create a new query for <typeparam name="T"/>
		/// </summary>
		IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName)
		{
			return Advanced.LuceneQuery<T>(indexName);
		}
	}
#endif
}
