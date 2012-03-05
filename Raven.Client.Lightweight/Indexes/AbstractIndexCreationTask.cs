//-----------------------------------------------------------------------
// <copyright file="AbstractIndexCreationTask.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
#if !NET_3_5
using System.Threading.Tasks;
using Raven.Client.Connection.Async;
#endif
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
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
#if !NET_3_5
	[System.ComponentModel.Composition.InheritedExport]
#endif
	public abstract class AbstractIndexCreationTask
	{
		/// <summary>
		/// Creates the index definition.
		/// </summary>
		/// <returns></returns>
		public abstract IndexDefinition CreateIndexDefinition();

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

#if !NET_3_5
		/// <summary>
		/// Allows to use lambdas recursively
		/// </summary>
		protected IEnumerable<TResult> Recurse<TSource,TResult>(TSource source, Func<TSource, TResult> func)
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
#endif

#if !SILVERLIGHT

		/// <summary>
		/// Executes the index creation against the specified document store.
		/// </summary>
		public void Execute(IDocumentStore store)
		{
			Execute(store.DatabaseCommands, store.Conventions);
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
			// the new defintion.
			databaseCommands.PutIndex(IndexName, indexDefinition, true);
		}
#endif

#if !NET_3_5
		/// <summary>
		/// Executes the index creation against the specified document store.
		/// </summary>
		public virtual Task ExecuteAsync(IAsyncDatabaseCommands asyncDatabaseCommands, DocumentConvention documentConvention)
		{
			Conventions = documentConvention;
			var indexDefinition = CreateIndexDefinition();
			// This code take advantage on the fact that RavenDB will turn an index PUT
			// to a noop of the index already exists and the stored definition matches
			// the new defintion.
			return asyncDatabaseCommands.PutIndexAsync(IndexName, indexDefinition, true);
		}
#endif
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
		/// <summary>
		/// Creates the index definition.
		/// </summary>
		/// <returns></returns>
		public override IndexDefinition CreateIndexDefinition()
		{
			return new IndexDefinitionBuilder<TDocument, TReduceResult>
			{
				Indexes = Indexes,
				SortOptions = IndexSortOptions,
				Analyzers = Analyzers,
				Map = Map,
				Reduce = Reduce,
				TransformResults = TransformResults,
				Stores = Stores
			}.ToIndexDefinition(Conventions);
		}

		/// <summary>
		/// The map definition
		/// </summary>
		protected Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }
	}
}
