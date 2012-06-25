using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
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
			StoresStrings = new Dictionary<string, FieldStorage>();
			Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
			IndexesStrings = new Dictionary<string, FieldIndexing>();
			IndexSortOptions = new Dictionary<Expression<Func<TReduceResult, object>>, SortOptions>();
			Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
			AnalyzersStrings = new Dictionary<string, string>();
		}

		protected internal override IEnumerable<object> ApplyReduceFunctionIfExists(IndexQuery indexQuey, IEnumerable<object> enumerable)
		{
			if (Reduce == null)
				return enumerable.Take(indexQuey.PageSize);
			
			var compile = Reduce.Compile();
			return compile(enumerable.Cast<TReduceResult>()).Cast<object>().Take(indexQuey.PageSize);
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
		/// Index storage options
		/// </summary>
		protected IDictionary<string, FieldStorage> StoresStrings
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
		/// Index sort options
		/// </summary>
		protected IDictionary<string, string> AnalyzersStrings
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
		/// Indexing options
		/// </summary>
		protected IDictionary<string, FieldIndexing> IndexesStrings
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
		/// Register a field to be indexed
		/// </summary>
		protected void Index(string field, FieldIndexing indexing)
		{
			IndexesStrings.Add(field, indexing);
		}

		/// <summary>
		/// Register a field to be stored
		/// </summary>
		protected void Store(Expression<Func<TReduceResult, object>> field, FieldStorage storage)
		{
			Stores.Add(field, storage);
		}

		protected void StoreAllFields(FieldStorage storage)
		{
			StoresStrings.Add(Constants.AllFields, storage);
		}

		/// <summary>
		/// Register a field to be stored
		/// </summary>
		protected void Store(string field, FieldStorage storage)
		{
			StoresStrings.Add(field, storage);
		}

		/// <summary>
		/// Register a field to be analyzed
		/// </summary>
		protected void Analyze(Expression<Func<TReduceResult, object>> field, string analyzer)
		{
			Analyzers.Add(field, analyzer);
		}

		/// <summary>
		/// Register a field to be analyzed
		/// </summary>
		protected void Analyze(string field, string analyzer)
		{
			AnalyzersStrings.Add(field, analyzer);
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