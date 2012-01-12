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
	public abstract class AbstractMultiMapIndexCreationTask<TReduceResult> : AbstractGenericIndexCreationTask<TReduceResult>
	{
		private readonly List<Func<string>> maps = new List<Func<string>>();

		protected void AddMap<TSource>(Expression<Func<IEnumerable<TSource>, IEnumerable>> expression)
		{
			maps.Add(() =>
			{
				string querySource = typeof(TSource) == typeof(object) ? "docs" : "docs." + Conventions.GetTypeTagName(typeof(TSource));
				return IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<TSource, TReduceResult>(expression, Conventions, querySource, translateIdentityProperty: true);
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
				SortOptions = IndexSortOptions,
				Analyzers = Analyzers,
				Reduce = Reduce,
				TransformResults = TransformResults,
				Stores = Stores
			}.ToIndexDefinition(Conventions, validateMap: false);
			foreach (var map in maps.Select(generateMap => generateMap()))
			{
				indexDefinition.Maps.Add(map);
			}
			return indexDefinition;
		}
	}

	/// <summary>
	/// Allow to create indexes with multiple maps
	/// </summary>
	public abstract class AbstractMultiMapIndexCreationTask : AbstractMultiMapIndexCreationTask<object>
	{
	}
}