//-----------------------------------------------------------------------
// <copyright file="IndexDefinition.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
		/// Get os set the analyzers
		/// </summary>
		public IDictionary<Expression<Func<TReduceResult, object>>, string> Analyzers { get; set; }
		/// <summary>
		/// Get os set the analyzers
		/// </summary>
		public IDictionary<string, string> AnalyzersStrings { get; set; }
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
			Analyzers = new Dictionary<Expression<Func<TReduceResult, object>>, string>();
			AnalyzersStrings = new Dictionary<string, string>();
		}

		/// <summary>
		/// Toes the index definition.
		/// </summary>
		public IndexDefinition ToIndexDefinition(DocumentConvention convention, bool validateMap = true)
		{
			if (Map == null && validateMap)
				throw new InvalidOperationException(
					string.Format("Map is required to generate an index, you cannot create an index without a valid Map property (in index {0}).", GetType().Name));

			string querySource = (typeof(TDocument) == typeof(object) || ContainsWhereEntityIs(Map.Body)) ? "docs" : "docs." + convention.GetTypeTagName(typeof(TDocument));
			var indexDefinition = new IndexDefinition
			{
				Reduce = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(Reduce, convention, "results", translateIdentityProperty: false),
				TransformResults = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(TransformResults, convention, "results", translateIdentityProperty: Reduce == null),
				Indexes = ConvertToStringDictionary(Indexes),
				Stores = ConvertToStringDictionary(Stores),
				SortOptions = ConvertToStringDictionary(SortOptions),
				Analyzers = ConvertToStringDictionary(Analyzers)
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
					throw new InvalidOperationException("There is a duplicate key in stores: " + analyzerString.Key);
				indexDefinition.Analyzers.Add(analyzerString);
			}

			if (Map != null)
				indexDefinition.Map = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TDocument, TReduceResult>(Map, convention,
																													 querySource,
																													 translateIdentityProperty
																														: true);

			return indexDefinition;
		}

#if !NET35
		private static bool ContainsWhereEntityIs(Expression body)
		{
			var whereEntityIsVisitor = new WhereEntityIsVisitor();
			whereEntityIsVisitor.Visit(body);
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
#else
		 private static bool ContainsWhereEntityIs(Expression body)
	    {
	        return body.ToString().Contains("WhereEntityIs");
	    }
#endif

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
