//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// This class provides a way to define a strongly typed index on the client.
	/// </summary>
	public class IndexDefinitionBuilder<TDocument, TReduceResult>
	{
		/// <summary>
		/// Gets or sets the map function
		/// </summary>
		/// <value>The map.</value>
		public Expression<Func<IEnumerable<TDocument>, IEnumerable>> Map { get; set; }
		/// <summary>
		/// Gets or sets the reduce function
		/// </summary>
		/// <value>The reduce.</value>
		public Expression<Func<IEnumerable<TReduceResult>, IEnumerable>> Reduce { get; set; }

		/// <summary>
		/// Gets or sets the reduce function
		/// </summary>
		/// <value>The reduce.</value>
		[Obsolete("Use Result Transformers instead.")]
		public Expression<Func<IClientSideDatabase, IEnumerable<TReduceResult>, IEnumerable>> TransformResults { get; set; }

		/// <summary>
		/// Gets or sets the stores options
		/// </summary>
		/// <value>The stores.</value>
		public IDictionary<Expression<Func<TReduceResult, object>>, FieldStorage> Stores { get; set; }

		/// <summary>
		/// Gets or sets the stores options
		/// </summary>
		/// <value>The stores.</value>
		public IDictionary<string, FieldStorage> StoresStrings { get; set; }

		/// <summary>
		/// Gets or sets the indexing options
		/// </summary>
		/// <value>The indexes.</value>
		public IDictionary<Expression<Func<TReduceResult, object>>, FieldIndexing> Indexes { get; set; }

		/// <summary>
		/// Gets or sets the indexing options
		/// </summary>
		/// <value>The indexes.</value>
		public IDictionary<string, FieldIndexing> IndexesStrings { get; set; }

		/// <summary>
		/// Gets or sets the sort options.
		/// </summary>
		/// <value>The sort options.</value>
		public IDictionary<Expression<Func<TReduceResult, object>>, SortOptions> SortOptions { get; set; }

		/// <summary>
        /// Gets or sets the sort options.
        /// </summary>
        /// <value>The sort options.</value>
        public Dictionary<string, SortOptions> SortOptionsStrings { get; set; }

		/// <summary>
		/// Get os set the analyzers
		/// </summary>
		public IDictionary<Expression<Func<TReduceResult, object>>, string> Analyzers { get; set; }

		/// <summary>
		/// Get os set the analyzers
		/// </summary>
		public IDictionary<string, string> AnalyzersStrings { get; set; }

		/// <summary>
		/// Gets or sets the suggestion options.
		/// </summary>
		/// <value>The suggestion options.</value>
		public IDictionary<Expression<Func<TReduceResult, object>>, SuggestionOptions> Suggestions { get; set; }

		/// <summary>
		/// Gets or sets the term vector options
		/// </summary>
		/// <value>The term vectors.</value>
		public IDictionary<Expression<Func<TReduceResult, object>>, FieldTermVector> TermVectors { get; set; }

		/// <summary>
		/// Gets or sets the term vector options
		/// </summary>
		/// <value>The term vectors.</value>
		public IDictionary<string, FieldTermVector> TermVectorsStrings { get; set; }

		/// <summary>
		/// Gets or sets the spatial options
		/// </summary>
		/// <value>The spatial options.</value>
		public IDictionary<Expression<Func<TReduceResult, object>>, SpatialOptions> SpatialIndexes { get; set; }

		/// <summary>
		/// Gets or sets the spatial options
		/// </summary>
		/// <value>The spatial options.</value>
		public IDictionary<string, SpatialOptions> SpatialIndexesStrings { get; set; }

		/// <summary>
		/// Max number of allowed indexing outputs per one source document
		/// </summary>
		public int? MaxIndexOutputsPerDocument { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexDefinitionBuilder{TDocument,TReduceResult}"/> class.
		/// </summary>
		public IndexDefinitionBuilder()
		{
			Stores = new Dictionary<Expression<Func<TReduceResult, object>>, FieldStorage>();
			StoresStrings = new Dictionary<string, FieldStorage>();
			Indexes = new Dictionary<Expression<Func<TReduceResult, object>>, FieldIndexing>();
			IndexesStrings = new Dictionary<string, FieldIndexing>();
			SortOptions = new Dictionary<Expression<Func<TReduceResult, object>>, SortOptions>();
            SortOptionsStrings = new Dictionary<string, SortOptions>();
			Suggestions = new Dictionary<Expression<Func<TReduceResult, object>>, SuggestionOptions>();
			Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
			AnalyzersStrings = new Dictionary<string, string>();
			TermVectors = new Dictionary<Expression<Func<TReduceResult, object>>, FieldTermVector>();
			TermVectorsStrings = new Dictionary<string, FieldTermVector>();
			SpatialIndexes = new Dictionary<Expression<Func<TReduceResult, object>>, SpatialOptions>();
			SpatialIndexesStrings = new Dictionary<string, SpatialOptions>();
		}

		/// <summary>
		/// Toes the index definition.
		/// </summary>
		public IndexDefinition ToIndexDefinition(DocumentConvention convention, bool validateMap = true)
		{
			if (Map == null && validateMap)
				throw new InvalidOperationException(
					string.Format("Map is required to generate an index, you cannot create an index without a valid Map property (in index {0}).", GetType().Name));

			if (Reduce != null)
				IndexDefinitionHelper.ValidateReduce(Reduce);

			string querySource = (typeof(TDocument) == typeof(object) || ContainsWhereEntityIs()) ? "docs" : "docs." + convention.GetTypeTagName(typeof(TDocument));
			var indexDefinition = new IndexDefinition
			{
				Reduce = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(Reduce, convention, "results", translateIdentityProperty: false),
#pragma warning disable 612,618
				TransformResults = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(TransformResults, convention, "results", translateIdentityProperty: Reduce == null),
#pragma warning restore 612,618
				Indexes = ConvertToStringDictionary(Indexes),
				Stores = ConvertToStringDictionary(Stores),
				SortOptions = ConvertToStringDictionary(SortOptions),
				Analyzers = ConvertToStringDictionary(Analyzers),
				Suggestions = ConvertToStringDictionary(Suggestions),
				TermVectors =  ConvertToStringDictionary(TermVectors),
				SpatialIndexes = ConvertToStringDictionary(SpatialIndexes),
				MaxIndexOutputsPerDocument = MaxIndexOutputsPerDocument
			};

			foreach (var indexesString in IndexesStrings)
			{
				if (indexDefinition.Indexes.ContainsKey(indexesString.Key))
					throw new InvalidOperationException("There is a duplicate key in indexes: " + indexesString.Key);
				indexDefinition.Indexes.Add(indexesString);
			}

			foreach (var storeString in StoresStrings)
			{
				if (indexDefinition.Stores.ContainsKey(storeString.Key))
					throw new InvalidOperationException("There is a duplicate key in stores: " + storeString.Key);
				indexDefinition.Stores.Add(storeString);
			}

			foreach (var analyzerString in AnalyzersStrings)
			{
				if (indexDefinition.Analyzers.ContainsKey(analyzerString.Key))
					throw new InvalidOperationException("There is a duplicate key in analyzers: " + analyzerString.Key);
				indexDefinition.Analyzers.Add(analyzerString);
			}

			foreach (var termVectorString in TermVectorsStrings)
			{
				if (indexDefinition.TermVectors.ContainsKey(termVectorString.Key))
					throw new InvalidOperationException("There is a duplicate key in term vectors: " + termVectorString.Key);
				indexDefinition.TermVectors.Add(termVectorString);
			}

			foreach (var spatialString in SpatialIndexesStrings)
			{
				if (indexDefinition.SpatialIndexes.ContainsKey(spatialString.Key))
					throw new InvalidOperationException("There is a duplicate key in spatial indexes: " + spatialString.Key);
				indexDefinition.SpatialIndexes.Add(spatialString);
			}

            foreach (var sortOption in SortOptionsStrings)
            {
                if (indexDefinition.SortOptions.ContainsKey(sortOption.Key))
                    throw new InvalidOperationException("There is a duplicate key in sort options: " + sortOption.Key);
                indexDefinition.SortOptions.Add(sortOption);
            }

			if (Map != null)
				indexDefinition.Map = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(Map, convention,
																													 querySource,
																													 translateIdentityProperty
																														: true);

			return indexDefinition;
		}

		private bool ContainsWhereEntityIs()
		{
		    if (Map == null) return false;
			var whereEntityIsVisitor = new WhereEntityIsVisitor();
			whereEntityIsVisitor.Visit(Map.Body);
			return whereEntityIsVisitor.HasWhereEntityIs;
		}

		private class WhereEntityIsVisitor : ExpressionVisitor
		{
			public bool HasWhereEntityIs { get; set; }

			protected override Expression VisitMethodCall(MethodCallExpression node)
			{
				if (node.Method.Name == "WhereEntityIs")
					HasWhereEntityIs = true;
				return base.VisitMethodCall(node);
			}
		}

		private static IDictionary<string, TValue> ConvertToStringDictionary<TValue>(IEnumerable<KeyValuePair<Expression<Func<TReduceResult, object>>, TValue>> input)
		{
			var result = new Dictionary<string, TValue>();
			foreach (var value in input)
			{
				var propertyPath = value.Key.ToPropertyPath('_');
				result[propertyPath] = value.Value;
			}
			return result;
		}
	}

	/// <summary>
	/// This class provides a way to define a strongly typed index on the client.
	/// </summary>
	public class IndexDefinitionBuilder<TDocument> : IndexDefinitionBuilder<TDocument, TDocument> { }
}
