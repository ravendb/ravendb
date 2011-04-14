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
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;

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

#if !NET_3_5
		/// <summary>
		/// Allows to use lambdas over dynamic
		/// </summary>
		protected IEnumerable<dynamic> Project<T>(IEnumerable<T> self, Func<T, object> projection)
		{
			throw new NotSupportedException("This can only be run on the server side");
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
	/// <remarks>
	/// The naming convention is that underscores in the inherited class names are replaced by slashed
	/// For example: Posts_ByName will be saved to Posts/ByName
	/// </remarks>
	public class AbstractIndexCreationTask<TDocument> :
		AbstractIndexCreationTask<TDocument, TDocument>
	{

	}

	/// <summary>
	/// Base class for creating indexes
	/// </summary>
	/// <remarks>
	/// The naming convention is that underscores in the inherited class names are replaced by slashed
	/// For example: Posts_ByName will be saved to Posts/ByName
	/// </remarks>
	public class AbstractIndexCreationTask<TDocument, TReduceResult> : AbstractIndexCreationTask
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
				SortOptions = SortOptions,
                Analyzers = Analyzers,
				Map = Map,
				Reduce = Reduce,
				TransformResults = TransformResults,
				Stores = Stores
			}.ToIndexDefinition(Conventions);
		}

		/// <summary>
		/// The result translator definition
		/// </summary>
		protected Expression<Func<IClientSideDatabase, IEnumerable<TReduceResult>, IEnumerable>> TransformResults { get; set; }

		/// <summary>
		/// The reduce definition
		/// </summary>
		protected Expression<Func<IEnumerable<TReduceResult>, IEnumerable>> Reduce { get; set; }


		/// <summary>
		/// The map definition
		/// </summary>
		protected Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }


		/// <summary>
		/// Index storage options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, FieldStorage> Stores
		{
			get;
			set;
		}


		/// <summary>
		/// Index sort options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, SortOptions> SortOptions
		{
			get;
			set;
		}


        /// <summary>
        /// Index sort options
        /// </summary>
        protected IDictionary<Expression<Func<TReduceResult, object>>, string> Analyzers
        {
            get;
            set;
        }


		/// <summary>
		/// Indexing options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, FieldIndexing> Indexes
		{
			get;
			set;
		}

		/// <summary>
		/// Create a new instance
		/// </summary>
		protected AbstractIndexCreationTask()
		{
			Stores = new Dictionary<Expression<Func<TReduceResult, object>>, FieldStorage>();
			Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
			SortOptions = new Dictionary<Expression<Func<TReduceResult, object>>, SortOptions>();
            Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
		}


		/// <summary>
		/// Register a field to be indexed
		/// </summary>
		protected void Index(Expression<Func<TReduceResult, object>> field, FieldIndexing indexing)
		{
			Indexes.Add(field, indexing);
		}

		/// <summary>
		/// Register a field to be stored
		/// </summary>
		protected void Store(Expression<Func<TReduceResult, object>> field, FieldStorage storage)
		{
			Stores.Add(field, storage);
		}

		/// <summary>
		/// Register a field to be sorted
		/// </summary>
		protected void Sort(Expression<Func<TReduceResult, object>> field, SortOptions sort)
		{
			SortOptions.Add(field, sort);
		}
	}
}
