using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Indexing;
using System.Linq;

namespace Raven.Client.Indexes
{
	/// <summary>
	/// Allow to create indexes with multiple maps
	/// </summary>
	public class AbstractMultiMapIndexCreationTask<TReduceResult> : AbstractGenericIndexCreationTask<TReduceResult>
	{
		private readonly List<Func<string>> maps = new List<Func<string>>();

		protected void AddMap<TSource>(Expression<Func<IEnumerable<TSource>, IEnumerable>> expression)
		{
			maps.Add(() =>
			{
				string querySource = typeof(TSource) == typeof(object) ? "docs" : "docs." + Conventions.GetTypeTagName(typeof(TSource));
				return IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TSource>(expression, Conventions, querySource, translateIdentityProperty: true);
			});
		}

		/// <summary>
		/// Creates the index definition.
		/// </summary>
		/// <returns></returns>
		public override IndexDefinition CreateIndexDefinition()
		{
			var indexDefinition = new IndexDefinitionBuilder<object, TReduceResult>
			{
				Indexes = Indexes,
				SortOptions = SortOptions,
				Analyzers = Analyzers,
				Reduce = Reduce,
				TransformResults = TransformResults,
				Stores = Stores
			}.ToIndexDefinition(Conventions, validateMap: false);
			indexDefinition.Maps.AddRange(maps.Select(generateMap=>generateMap()));
			return indexDefinition;
		}
	}

	/// <summary>
	/// Allow to create indexes with multiple maps
	/// </summary>
	public class AbstractMultiMapIndexCreationTask : AbstractMultiMapIndexCreationTask<object>
	{
	}
}