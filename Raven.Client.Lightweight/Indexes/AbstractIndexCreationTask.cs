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
	/// <summary>
	/// Base class for creating indexes
	/// </summary>
	/// <remarks>
	/// The naming convention is that underscores in the inherited class names are replaced by slashed
	/// For example: Posts_ByName will be saved to Posts/ByName
	/// </remarks>
	[System.ComponentModel.Composition.InheritedExport]
	public abstract class AbstractIndexCreationTask
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
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordiates
		/// </summary>
		/// <param name="lat">Latitude</param>
		/// <param name="lng">Longitude</param>
		/// <returns></returns>
		public static object SpatialGenerate(double lat, double lng)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordiates
		/// </summary>
		/// <param name="fieldName">The field name, will be used for querying</param>
		/// <param name="lat">Latitude</param>
		/// <param name="lng">Longitude</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, double lat, double lng)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		protected class SpatialIndex
		{
			/// <summary>
			/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordiates
			/// </summary>
			/// <param name="fieldName">The field name, will be used for querying</param>
			/// <param name="lat">Latitude</param>
			/// <param name="lng">Longitude</param>
			public static object Generate(string fieldName, double lat, double lng)
			{
				throw new NotSupportedException("This method is provided solely to allow query translation on the server");
			}

			/// <summary>
			/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordiates
			/// </summary>
			/// <param name="lat">Latitude</param>
			/// <param name="lng">Longitude</param>
			public static object Generate(double lat, double lng)
			{
				throw new NotSupportedException("This method is provided solely to allow query translation on the server");
			}
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordiates
		/// </summary>
		/// <param name="fieldName">The field name, will be uased for querying</param>
		/// <param name="shapeWKT">The shape representatio in the WKT format</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, string shapeWKT)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordiates
		/// </summary>
		/// <param name="fieldName">The field name, will be uased for querying</param>
		/// <param name="shapeWKT">The shape representatio in the WKT format</param>
		/// <param name="strategy">The spatial strategy to use</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, string shapeWKT, SpatialSearchStrategy strategy)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Generates a spatial field in the index, generating a Point from the provided lat/lng coordiates
		/// </summary>
		/// <param name="fieldName">The field name, will be uased for querying</param>
		/// <param name="shapeWKT">The shape representatio in the WKT format</param>
		/// <param name="strategy">The spatial strategy to use</param>
		/// <param name="maxTreeLevel">Maximum number of levels to be used in the PrefixTree, controls the precision of shape representation.</param>
		/// <returns></returns>
		public static object SpatialGenerate(string fieldName, string shapeWKT, SpatialSearchStrategy strategy, int maxTreeLevel)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

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

#if !SILVERLIGHT
		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource, TResult>(TSource source, Func<TSource, SortedSet<TResult>> func)
		{
			throw new NotSupportedException("This can only be run on the server side");
		}
#endif

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
		/// Allow to get to the metadata of the document
		/// </summary>
		protected RavenJObject AsDocument(object doc)
		{
			throw new NotSupportedException("This is here as a marker only");
		}

#if !SILVERLIGHT

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

			UpdateIndexInReplication(databaseCommands, documentConvention, indexDefinition);
		}

		private void UpdateIndexInReplication(IDatabaseCommands databaseCommands, DocumentConvention documentConvention,
											  IndexDefinition indexDefinition)
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
					serverClient.DirectPutIndex(IndexName, replicationDestination.Url, true, indexDefinition);
				}
				catch (Exception e)
				{
					Logger.WarnException("Could not put index in replication server", e);
				}
			}
		}
#endif

		/// <summary>
		/// Executes the index creation against the specified document store.
		/// </summary>
		public virtual Task ExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention)
		{
			Conventions = documentConvention;
			var indexDefinition = CreateIndexDefinition();
			// This code take advantage on the fact that RavenDB will turn an index PUT
			// to a noop of the index already exists and the stored definition matches
			// the new definition.
			return asyncDatabaseCommands.PutIndexAsync(IndexName, indexDefinition, true)
				.ContinueWith(task => UpdateIndexInReplicationAsync(asyncDatabaseCommands, documentConvention, indexDefinition))
				.Unwrap();
		}

		private ILog Logger = LogManager.GetCurrentClassLogger();
		private Task UpdateIndexInReplicationAsync(IAsyncDatabaseCommands asyncDatabaseCommands,
												   DocumentConvention documentConvention, IndexDefinition indexDefinition)
		{
			var asyncServerClient = asyncDatabaseCommands as AsyncServerClient;
			if (asyncServerClient == null)
				return new CompletedTask();
			return asyncServerClient.GetAsync("Raven/Replication/Destinations").ContinueWith(doc =>
			{
				if (doc == null)
					return new CompletedTask();
				var replicationDocument =
					documentConvention.CreateSerializer().Deserialize<ReplicationDocument>(new RavenJTokenReader(doc.Result.DataAsJson));
				if (replicationDocument == null)
					return new CompletedTask();
				var tasks = new List<Task>();
				foreach (var replicationDestination in replicationDocument.Destinations)
				{
					tasks.Add(asyncServerClient.DirectPutIndexAsync(IndexName, indexDefinition, true, replicationDestination.Url));
				}
				return Task.Factory.ContinueWhenAll(tasks.ToArray(), indexingTask =>
				{
					foreach (var indexTask in indexingTask)
					{
						if (indexTask.IsFaulted)
						{
							Logger.WarnException("Could not put index in replication server", indexTask.Exception);
						}
					}
				});
			}).Unwrap();
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
			return new IndexDefinitionBuilder<TDocument, TReduceResult>
			{
				Indexes = Indexes,
				IndexesStrings = IndexesStrings,
				SortOptions = IndexSortOptions,
				Analyzers = Analyzers,
				AnalyzersStrings = AnalyzersStrings,
				Map = Map,
				Reduce = Reduce,
				TransformResults = TransformResults,
				Stores = Stores,
				StoresStrings = StoresStrings
			}.ToIndexDefinition(Conventions);
		}

		/// <summary>
		/// The map definition
		/// </summary>
		protected Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }
	}
}
