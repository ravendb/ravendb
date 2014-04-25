//-----------------------------------------------------------------------
// <copyright file="AbstractIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Indexes
{
	using System.Net;

	/// <summary>
	/// Base class for creating indexes
	/// </summary>
	/// <remarks>
	/// The naming convention is that underscores in the inherited class names are replaced by slashed
	/// For example: Posts_ByName will be saved to Posts/ByName
	/// </remarks>
#if !MONO && !NETFX_CORE
	[System.ComponentModel.Composition.InheritedExport]
#endif
	public abstract class AbstractIndexCreationTask : AbstractCommonApiForIndexesAndTransformers
	{
		/// <summary>
		/// Creates the index definition.
		/// </summary>
		/// <returns></returns>
		public abstract IndexDefinition CreateIndexDefinition();

		protected internal virtual IEnumerable<object> ApplyReduceFunctionIfExists(IndexQuery indexQuery, IEnumerable<object> enumerable)
		{
			return enumerable.Take(indexQuery.PageSize);
		}

		public virtual bool IsMapReduce { get { return false; } }

		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
		public virtual string IndexName { get { return GetType().Name.Replace("_", "/"); } }

		/// <summary>
		/// Gets or sets the document store.
		/// </summary>
		/// <value>The document store.</value>
		public DocumentConvention Conventions { get; set; }

		/// <summary>
		/// Provide a way to dynamically index values with runtime known values
		/// </summary>
		protected object CreateField(string name, object value, bool stored, bool analyzed)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Provide a way to dynamically index values with runtime known values
		/// </summary>
		protected object CreateField(string name, object value)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
		/// </summary>
		/// <param name="lat">Latitude</param>
		/// <param name="lng">Longitude</param>
		/// <returns></returns>
		public static object SpatialGenerate(double? lat, double? lng)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generate field with values that can be used for spatial clustering on the lat/lng coordinates
		/// </summary>
		public object SpatialClustering(string fieldName, double? lat, double? lng)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generate field with values that can be used for spatial clustering on the lat/lng coordinates
		/// </summary>
		public object SpatialClustering(string fieldName, double? lat, double? lng,
		                                                 int minPrecision,
		                                                 int maxPrecision)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
		/// </summary>
		/// <param name="fieldName">The field name, will be used for querying</param>
		/// <param name="lat">Latitude</param>
		/// <param name="lng">Longitude</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, double? lat, double? lng)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		[Obsolete]
		protected class SpatialIndex
		{
			/// <summary>
			/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
			/// </summary>
			/// <param name="fieldName">The field name, will be used for querying</param>
			/// <param name="lat">Latitude</param>
			/// <param name="lng">Longitude</param>
			[Obsolete("Use SpatialGenerate instead.")]
			public static object Generate(string fieldName, double? lat, double? lng)
			{
				throw new NotSupportedException("This method is provided solely to allow query translation on the server");
			}

			/// <summary>
			/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
			/// </summary>
			/// <param name="lat">Latitude</param>
			/// <param name="lng">Longitude</param>
			[Obsolete("Use SpatialGenerate instead.")]
			public static object Generate(double? lat, double? lng)
			{
				throw new NotSupportedException("This method is provided solely to allow query translation on the server");
			}
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
		/// </summary>
		/// <param name="fieldName">The field name, will be used for querying</param>
		/// <param name="shapeWKT">The shape representation in the WKT format</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, string shapeWKT)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
		/// </summary>
		/// <param name="fieldName">The field name, will be used for querying</param>
		/// <param name="shapeWKT">The shape representation in the WKT format</param>
		/// <param name="strategy">The spatial strategy to use</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, string shapeWKT, SpatialSearchStrategy strategy)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordinates
		/// </summary>
		/// <param name="fieldName">The field name, will be used for querying</param>
		/// <param name="shapeWKT">The shape representation in the WKT format</param>
		/// <param name="strategy">The spatial strategy to use</param>
		/// <param name="maxTreeLevel">Maximum number of levels to be used in the PrefixTree, controls the precision of shape representation.</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, string shapeWKT, SpatialSearchStrategy strategy, int maxTreeLevel)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Loads the specifed document during the indexing process
		/// </summary>
		public object LoadAttachmentForIndexing(string key)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

#if !NETFX_CORE

		/// <summary>
		/// Executes the index creation against the specified document store.
		/// </summary>
		public void Execute(IDocumentStore store)
		{
			store.ExecuteIndex(this);
		}

		/// <summary>
		/// Executes the index creation against the specified document database using the specified conventions
		/// </summary>
		public virtual void Execute(IDatabaseCommands databaseCommands, DocumentConvention documentConvention)
		{
			Conventions = documentConvention;
			var indexDefinition = CreateIndexDefinition();
			// This code take advantage on the fact that RavenDB will turn an index PUT
			// to a noop of the index already exists and the stored definition matches
			// the new definition.
			databaseCommands.PutIndex(IndexName, indexDefinition, true);

			UpdateIndexInReplication(databaseCommands, documentConvention, (commands, url) =>
				commands.DirectPutIndex(IndexName, url, true, indexDefinition));
		}

#endif

		/// <summary>
		/// Executes the index creation against the specified document store.
		/// </summary>
		public virtual async Task ExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention)
		{
			Conventions = documentConvention;
			var indexDefinition = CreateIndexDefinition();
			
			// This code take advantage on the fact that RavenDB will turn an index PUT
			// to a noop of the index already exists and the stored definition matches
			// the new definition.
			await asyncDatabaseCommands.PutIndexAsync(IndexName, indexDefinition, true);
			await UpdateIndexInReplicationAsync(asyncDatabaseCommands, documentConvention, (client, operationMetadata) => client.DirectPutIndexAsync(IndexName, indexDefinition, true, operationMetadata));				
		}
	}

	/// <summary>
	/// Base class for creating indexes
	/// </summary>
	public class AbstractIndexCreationTask<TDocument> :
		AbstractIndexCreationTask<TDocument, TDocument>
	{

	}

	/// <summary>
	/// Base class for creating indexes
	/// </summary>
	public class AbstractIndexCreationTask<TDocument, TReduceResult> : AbstractGenericIndexCreationTask<TReduceResult>
	{
		protected internal override IEnumerable<object> ApplyReduceFunctionIfExists(IndexQuery indexQuery, IEnumerable<object> enumerable)
		{
			if (Reduce == null)
				return enumerable.Take(indexQuery.PageSize);

			return Conventions.ApplyReduceFunction(GetType(), typeof(TReduceResult), enumerable, () =>
			{
				var compile = Reduce.Compile();
				return (objects => compile(objects.Cast<TReduceResult>()));
			}).Take(indexQuery.PageSize);
		}

		/// <summary>
		/// Creates the index definition.
		/// </summary>
		/// <returns></returns>
		public override IndexDefinition CreateIndexDefinition()
		{
			if (Conventions == null)
				Conventions = new DocumentConvention();


			return new IndexDefinitionBuilder<TDocument, TReduceResult>
			{
				Indexes = Indexes,
				IndexesStrings = IndexesStrings,
                SortOptionsStrings = IndexSortOptionsStrings,
				SortOptions = IndexSortOptions,
				Analyzers = Analyzers,
				AnalyzersStrings = AnalyzersStrings,
				Map = Map,
				Reduce = Reduce,
#pragma warning disable 612,618
				TransformResults = TransformResults,
#pragma warning restore 612,618
				Stores = Stores,
				StoresStrings = StoresStrings,
				Suggestions = IndexSuggestions,
				TermVectors = TermVectors,
				TermVectorsStrings = TermVectorsStrings,
				SpatialIndexes = SpatialIndexes,
				SpatialIndexesStrings = SpatialIndexesStrings,
                DisableInMemoryIndexing = DisableInMemoryIndexing,
				MaxIndexOutputsPerDocument = MaxIndexOutputsPerDocument
			}.ToIndexDefinition(Conventions);
		}

		/// <summary>
		/// Max number of allowed indexing outputs per one source document
		/// </summary>
		public int? MaxIndexOutputsPerDocument { get; set; }

		public override bool IsMapReduce
		{
			get { return Reduce != null; }
		}

		/// <summary>
		/// The map definition
		/// </summary>
		protected Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }
	}

	public abstract class AbstractCommonApiForIndexesAndTransformers
	{
		private ILog Logger = LogManager.GetCurrentClassLogger();

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, TResult> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}


		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, IEnumerable<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, ICollection<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, ISet<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, HashSet<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, SortedSet<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Loads the specifed document during the indexing process
		/// </summary>
		public T LoadDocument<T>(string key)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Loads the specifed document during the indexing process
		/// </summary>
		public T[] LoadDocument<T>(IEnumerable<string> keys)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, IList<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, TResult[]> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, List<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}

		/// <summary>
		/// Allow to get to the metadata of the document
		/// </summary>
		protected RavenJObject MetadataFor(object doc)
		{
			throw new NotSupportedException("This is here as a marker only");
		}

		/// <summary>
		/// Allow to access an entity as a document
		/// </summary>
		protected RavenJObject AsDocument(object doc)
		{
			throw new NotSupportedException("This is here as a marker only");
		}

		internal async Task UpdateIndexInReplicationAsync(IAsyncDatabaseCommands asyncDatabaseCommands,
												   DocumentConvention documentConvention, 
                                                    Func<AsyncServerClient, OperationMetadata, Task> action)
		{
		    var asyncServerClient = asyncDatabaseCommands as AsyncServerClient;
		    if (asyncServerClient == null)
		        return;
		    var doc = await asyncServerClient.GetAsync("Raven/Replication/Destinations");
		    if (doc == null)
		        return;
		    var replicationDocument =
		        documentConvention.CreateSerializer().Deserialize<ReplicationDocument>(new RavenJTokenReader(doc.DataAsJson));
            if (replicationDocument == null || replicationDocument.Destinations == null || replicationDocument.Destinations.Count == 0)
		        return;
		    var tasks = (
                         from replicationDestination in replicationDocument.Destinations
		                 where !replicationDestination.Disabled && !replicationDestination.IgnoredClient
		                 select action(asyncServerClient, GetReplicationOperation(replicationDestination))
                         )
                         .ToArray();
		    await Task.Factory.ContinueWhenAll(tasks, indexingTask =>
		    {
		        foreach (var indexTask in indexingTask)
		        {
		            if (indexTask.IsFaulted)
		            {
		                Logger.WarnException("Could not put index in replication server", indexTask.Exception);
		            }
		        }
		    });
		}

	    private OperationMetadata GetReplicationOperation(ReplicationDestination replicationDestination)
		{
			var replicationUrl = replicationDestination.ClientVisibleUrl ?? replicationDestination.Url;
			var url = string.IsNullOrWhiteSpace(replicationDestination.Database)
				? replicationUrl
				: replicationUrl + "/databases/" + replicationDestination.Database;

			return new OperationMetadata(url, replicationDestination.Username, replicationDestination.Password, replicationDestination.Domain, replicationDestination.ApiKey);
		}

#if !NETFX_CORE
		internal void UpdateIndexInReplication(IDatabaseCommands databaseCommands, DocumentConvention documentConvention,
			Action<ServerClient, OperationMetadata> action)
		{
			var serverClient = databaseCommands as ServerClient;
			if (serverClient == null)
				return;
			var doc = serverClient.Get("Raven/Replication/Destinations");
			if (doc == null)
				return;
			var replicationDocument =
				documentConvention.CreateSerializer().Deserialize<ReplicationDocument>(new RavenJTokenReader(doc.DataAsJson));
			if (replicationDocument == null)
				return;

			foreach (var replicationDestination in replicationDocument.Destinations)
			{
				try
				{
					if (replicationDestination.Disabled || replicationDestination.IgnoredClient)
						continue;
					action(serverClient, GetReplicationOperation(replicationDestination));
				}
				catch (Exception e)
				{
					Logger.WarnException("Could not put index in replication server", e);
				}
			}
		}
#endif
	}
}
