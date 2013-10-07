using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Indexing;
using System.Linq;
using Raven.Client.Document;
#if !NETFX_CORE
using Raven.Abstractions.MissingFromBCL;
#else
using Raven.Client.WinRT.MissingFromWinRT;
using Raven.Imports.Newtonsoft.Json.Utilities;
#endif
using Raven.Client.Linq;

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
		/// Uses reflection to call <see cref="AddMap{TSource}"/> for the base type and all available subclasses.
		/// </summary>
		/// <remarks>This is taken from Oren's code in this thread https://groups.google.com/d/msg/ravendb/eFUlQG-spzE/Ac0PrvsFyJYJ </remarks>
		/// <typeparam name="TBase">The base class type whose descendant types are to be included in the index.</typeparam>
		/// <param name="expr"></param>
		protected void AddMapForAll<TBase>(Expression<Func<IEnumerable<TBase>, IEnumerable>> expr)
		{
			// Index the base class.
			AddMap(expr);

			// Index child classes.
			var children = typeof(TBase).Assembly().GetTypes().Where(x => typeof(TBase).IsAssignableFrom(x));
			var addMapGeneric = GetType().GetMethod("AddMap", BindingFlags.Instance | BindingFlags.NonPublic);
			foreach (var child in children)
			{
				if (child.IsGenericTypeDefinition)
					continue;
				var genericEnumerable = typeof(IEnumerable<>).MakeGenericType(child.AsType());
				var delegateType = typeof(Func<,>).MakeGenericType(genericEnumerable, typeof(IEnumerable));
				var lambdaExpression = Expression.Lambda(delegateType, expr.Body, Expression.Parameter(genericEnumerable, expr.Parameters[0].Name));
				addMapGeneric.MakeGenericMethod(child.AsType()).Invoke(this, new[] { lambdaExpression });
			}
		}

		/// <summary>
		/// Creates the index definition.
		/// </summary>
		/// <returns></returns>
		public override IndexDefinition CreateIndexDefinition()
		{
			if (Conventions == null)
				Conventions = new DocumentConvention();

			var indexDefinition = new IndexDefinitionBuilder<object, TReduceResult>
			{
				Indexes = Indexes,
				SortOptions = IndexSortOptions,
                SortOptionsStrings = IndexSortOptionsStrings,
				Analyzers = Analyzers,
				Reduce = Reduce,
#pragma warning disable 612,618
				TransformResults = TransformResults,
#pragma warning restore 612,618
				Stores = Stores,
				TermVectors = TermVectors,
				SpatialIndexes = SpatialIndexes,
				Suggestions = IndexSuggestions,
				AnalyzersStrings = AnalyzersStrings,
				IndexesStrings = IndexesStrings,
				StoresStrings = StoresStrings,
				TermVectorsStrings = TermVectorsStrings,
				SpatialIndexesStrings = SpatialIndexesStrings
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
