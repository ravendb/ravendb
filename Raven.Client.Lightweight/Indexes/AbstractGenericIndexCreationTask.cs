using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Indexing;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Abstract class used to provide infrastructure service for actual creation tasks
	/// </summary>
	public abstract class AbstractGenericIndexCreationTask<TReduceResult> : AbstractIndexCreationTask
	{
		/// <summary>
		/// Create a new instance
		/// </summary>
		protected AbstractGenericIndexCreationTask()
		{
			Stores = new Dictionary<Expression<Func<TReduceResult, object>>, FieldStorage>();
			Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
			IndexSortOptions = new Dictionary<Expression<Func<TReduceResult, object>>, SortOptions>();
			Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
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
		protected IDictionary<Expression<Func<TReduceResult, object>>, SortOptions> IndexSortOptions
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
			IndexSortOptions.Add(field, sort);
		}
	}
}