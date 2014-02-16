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
            IndexSortOptionsStrings = new Dictionary<string, SortOptions>();
			Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
			AnalyzersStrings = new Dictionary<string, string>();
			IndexSuggestions = new Dictionary<Expression<Func<TReduceResult, object>>, SuggestionOptions>();
			TermVectors = new Dictionary<Expression<Func<TReduceResult, object>>, FieldTermVector>();
			TermVectorsStrings = new Dictionary<string, FieldTermVector>();
			SpatialIndexes = new Dictionary<Expression<Func<TReduceResult, object>>, SpatialOptions>(); 
			SpatialIndexesStrings = new Dictionary<string, SpatialOptions>();
		}

		public override bool IsMapReduce
		{
			get { return Reduce != null; }
		}

		protected internal override IEnumerable<object> ApplyReduceFunctionIfExists(IndexQuery indexQuery, IEnumerable<object> enumerable)
		{
			if (Reduce == null)
				return enumerable.Take(indexQuery.PageSize);

			var compile = Reduce.Compile();
			return compile(enumerable.Cast<TReduceResult>()).Cast<object>().Take(indexQuery.PageSize);
		}

		/// <summary>
		/// The result translator definition
		/// </summary>
		[Obsolete("Use Result Transformers instead.")]
		protected Expression<Func<IClientSideDatabase, IEnumerable<TReduceResult>, IEnumerable>> TransformResults { get; set; }

		/// <summary>
		/// The reduce definition
		/// </summary>
		protected Expression<Func<IEnumerable<TReduceResult>, IEnumerable>> Reduce { get; set; }

		/// <summary>
		/// Index storage options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, FieldStorage> Stores { get; set; }

		/// <summary>
		/// Index storage options
		/// </summary>
		protected IDictionary<string, FieldStorage> StoresStrings { get; set; }

		/// <summary>
		/// Index sort options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, SortOptions> IndexSortOptions { get; set; }

        /// <summary>
        /// Index sort options
        /// </summary>
        protected Dictionary<string, SortOptions> IndexSortOptionsStrings { get; set; }

		/// <summary>
		/// Index suggest options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, SuggestionOptions> IndexSuggestions { get; set; }

		/// <summary>
		/// Index sort options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, string> Analyzers { get; set; }

		/// <summary>
		/// Index sort options
		/// </summary>
		protected IDictionary<string, string> AnalyzersStrings { get; set; }

		/// <summary>
		/// Index term vector options
		/// </summary>		
		protected IDictionary<Expression<Func<TReduceResult, object>>, FieldTermVector> TermVectors { get; set; }

		/// <summary>
		/// Index term vector options
		/// </summary>		
		protected IDictionary<string, FieldTermVector> TermVectorsStrings { get; set; }

		/// <summary>
		/// Spatial index options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, SpatialOptions> SpatialIndexes { get; set; }

		/// <summary>
		/// Spatial index options
		/// </summary>
		protected IDictionary<string, SpatialOptions> SpatialIndexesStrings { get; set; }


		/// <summary>
		/// Indexing options
		/// </summary>
		protected IDictionary<Expression<Func<TReduceResult, object>>, FieldIndexing> Indexes { get; set; }

		/// <summary>
		/// Indexing options
		/// </summary>
		protected IDictionary<string, FieldIndexing> IndexesStrings { get; set; }

		/// <summary>
		/// Prevent index from being kept in memory. Default: false
		/// </summary>
		public bool DisableInMemoryIndexing { get; set; }

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
		/// Register a field to be spatially indexed
		/// </summary>
		protected void Spatial(Expression<Func<TReduceResult, object>> field, Func<SpatialOptionsFactory, SpatialOptions> indexing)
		{
			SpatialIndexes.Add(field, indexing(new SpatialOptionsFactory()));
		}

		/// <summary>
		/// Register a field to be spatially indexed
		/// </summary>
		protected void Spatial(string field, Func<SpatialOptionsFactory, SpatialOptions> indexing)
		{
			SpatialIndexesStrings.Add(field, indexing(new SpatialOptionsFactory()));
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
		/// Register a field to have term vectors
		/// </summary>
		protected void TermVector(Expression<Func<TReduceResult, object>> field, FieldTermVector termVector)
		{
			TermVectors.Add(field, termVector);
		}

		/// <summary>
		/// Register a field to have term vectors
		/// </summary>
		protected void TermVector(string field, FieldTermVector termVector)
		{
			TermVectorsStrings.Add(field, termVector);
		}

		/// <summary>
		/// Register a field to be sorted
		/// </summary>
		protected void Sort(Expression<Func<TReduceResult, object>> field, SortOptions sort)
		{
			IndexSortOptions.Add(field, sort);
		}

        /// <summary>
        /// Register a field to be sorted
        /// </summary>
        protected void Sort(string field, SortOptions sort)
        {
            IndexSortOptionsStrings.Add(field, sort);
        }

		/// <summary>
		/// Register a field to be sorted
		/// </summary>
		protected void Suggestion(Expression<Func<TReduceResult, object>> field, SuggestionOptions suggestion = null)
		{
			IndexSuggestions.Add(field, suggestion ?? new SuggestionOptions
			{
				Accuracy = 0.5f,
				Distance = StringDistanceTypes.Levenshtein
			});
		}
	}
}