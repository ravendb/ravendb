//-----------------------------------------------------------------------
// <copyright file="LinqExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Reflection;
using System.Linq;
#if !NET_3_5
using System.Threading.Tasks;
#endif
using Raven.Abstractions.Data;
using Raven.Client.Connection;

namespace Raven.Client.Linq
{
	using System.Linq.Expressions;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;

	///<summary>
	/// Extensions to the linq syntax
	///</summary>
	public static partial class LinqExtensions
	{
#if !SILVERLIGHT
		/// <summary>
		/// Query the facets results for this query using the specified facet document
		/// </summary>
		public static IDictionary<string, IEnumerable<FacetValue>> ToFacets<T>(this IQueryable<T> queryable, string facetDoc)
		{
			var ravenQueryInspector = ((RavenQueryInspector<T>)queryable);
			var query = ravenQueryInspector.ToString();
			var provider = queryable.Provider as IRavenQueryProvider;

			return ravenQueryInspector.DatabaseCommands.GetFacets(ravenQueryInspector.IndexQueried,
																  new IndexQuery { Query = query }, facetDoc);
		}
#endif
#if !NET_3_5
		/// <summary>
		/// Query the facets results for this query using the specified facet document
		/// </summary>
		public static Task<IDictionary<string, IEnumerable<FacetValue>>> ToFacetsAsync<T>(this IQueryable<T> queryable, string facetDoc)
		{
			var ravenQueryInspector = ((RavenQueryInspector<T>)queryable);
			var query = ravenQueryInspector.ToString();
			var provider = queryable.Provider as IRavenQueryProvider;

			return ravenQueryInspector.AsyncDatabaseCommands.GetFacetsAsync(ravenQueryInspector.IndexQueried,
																  new IndexQuery { Query = query }, facetDoc);
		}
#endif

		/// <summary>
		/// Project using a different type
		/// </summary>
		public static IQueryable<TResult> As<TResult>(this IQueryable queryable)
		{
			var ofType = queryable.OfType<TResult>();
			var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
			var ravenQueryInspector = ((RavenQueryInspector<TResult>)results);
			ravenQueryInspector.Customize(x => x.CreateQueryForSelectedFields<TResult>(null));
			return results;
		}

		/// <summary>
		/// Project using a different type
		/// </summary>
		public static IRavenQueryable<TResult> AsProjection<TResult>(this IQueryable queryable)
		{
			var ofType = queryable.OfType<TResult>();
			var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
			var ravenQueryInspector = ((RavenQueryInspector<TResult>)results);
			ravenQueryInspector.FieldsToFetch(typeof(TResult).GetProperties().Select(x => x.Name));
			ravenQueryInspector.Customize(x => x.CreateQueryForSelectedFields<TResult>(null));
			return (IRavenQueryable<TResult>)results;
		}
#if !SILVERLIGHT

		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static SuggestionQueryResult Suggest(this IQueryable queryable)
		{
			return Suggest(queryable, new SuggestionQuery());
		}

		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static SuggestionQueryResult Suggest(this IQueryable queryable, SuggestionQuery query)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			SetSuggestionQueryFieldAndTerm(ravenQueryInspector,query);
			return ravenQueryInspector.DatabaseCommands.Suggest(ravenQueryInspector.IndexQueried, query);
		}
#endif

		private static void SetSuggestionQueryFieldAndTerm(IRavenQueryInspector queryInspector, SuggestionQuery query)
		{
			var lastEqualityTerm = queryInspector.GetLastEqualityTerm();
			if (lastEqualityTerm.Key == null)
				throw new InvalidOperationException("Could not suggest on a query that doesn't have a single equality check");

			query.Field = lastEqualityTerm.Key;
			query.Term = lastEqualityTerm.Value;
		}
#if !NET_3_5
		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static Task<SuggestionQueryResult> SuggestAsync(this IQueryable queryable, SuggestionQuery query)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query);

			return ravenQueryInspector.AsyncDatabaseCommands.SuggestAsync(ravenQueryInspector.IndexQueried, query);
		}

		/// <summary>
		/// Suggest alternative values for the queried term
		/// </summary>
		public static Task<SuggestionQueryResult> SuggestAsync(this IQueryable queryable)
		{
			return SuggestAsync(queryable, new SuggestionQuery());
		}
#endif


		/// <summary>
		/// Perform a search for documents which fields that match the searchTerms.
		/// If there is more than a single term, each of them will be checked independently.
		/// </summary>
		public static IRavenQueryable<T> Search<T>(this IRavenQueryable<T> self, Expression<Func<T, object>> fieldSelector, string searchTerms)
		{
			var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
			var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof (T)), self.Expression,
			                                                          fieldSelector, Expression.Constant(searchTerms)));
			return (IRavenQueryable<T>)queryable;
		}

		/// <summary>
		/// Marker method for allowing complex (multi entity) queries on the server.
		/// </summary>
		public static IEnumerable<TResult> WhereEntityIs<TResult>(this IEnumerable<object> queryable, params string[] names)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Marker method for allowing complex (multi entity) queries on the server.
		/// </summary>
		public static TResult IfEntityIs<TResult>(this object queryable, string name)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Marker method for allowing hierarchical queries on the server.
		/// </summary>
		public static IEnumerable<TResult> Hierarchy<TResult>(this TResult item, string path)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

		/// <summary>
		/// Marker method for allowing hierarchical queries on the server.
		/// </summary>
		public static IEnumerable<TResult> Hierarchy<TResult>(this TResult item, Func<TResult, IEnumerable<TResult>> path)
		{
			throw new NotSupportedException("This method is provided solely to allow query translation on the server");
		}

#if !NET_3_5
		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		public static Lazy<IEnumerable<T>> Lazily<T>(this IQueryable<T> source)
		{
			return Lazily(source, null);
		}

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// As well as a function to execute when the value is evaluated
		/// </summary>
		public static Lazy<IEnumerable<T>> Lazily<T>(this IQueryable<T> source, Action<IEnumerable<T>> onEval)
		{
			var provider = source.Provider as IRavenQueryProvider;
			if (provider == null)
				throw new ArgumentException("You can only use Raven Queryable with Lazily");

			return provider.Lazily(source.Expression, onEval);
		}

		/// <summary>
		/// Returns a list of results for a query asynchronously. 
		/// </summary>
		public static Task<IList<T>> ToListAsync<T>(this IQueryable<T> source)
		{
			var provider = source.Provider as IRavenQueryProvider;
			if(provider == null)
				throw new ArgumentException("You can only use Raven Queryable with ToListAsync");
			var documentQuery = provider.ToAsyncLuceneQuery<T>(source.Expression);
			provider.MoveAfterQueryExecuted(documentQuery);
			return documentQuery.ToListAsync()
				.ContinueWith(task => task.Result.Item2);
		}

		/// <summary>
		/// Returns the total count of results for a query asynchronously. 
		/// </summary>
		public static Task<int> CountAsync<T>(this IQueryable<T> source)
		{
			var provider = source.Provider as IRavenQueryProvider;
			if (provider == null)
				throw new ArgumentException("You can only use Raven Queryable with CountAsync");
			
			var documentQuery = provider
				.ToAsyncLuceneQuery<T>(source.Expression)
				.Take(0);
			provider.MoveAfterQueryExecuted(documentQuery);
			return documentQuery.ToListAsync()
				.ContinueWith(task => task.Result.Item1.TotalResults);
		} 
#endif

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		public static IRavenQueryable<T> Include<T>(this IRavenQueryable<T> source, Expression<Func<T, object>> path)
		{
			source.Customize(x => x.Include(path));
			return source;
		}

		/// <summary>
		/// Filters a sequence of values based on a predicate.
		/// </summary>
		public static IRavenQueryable<T> Where<T>(this IRavenQueryable<T> source, Expression<Func<T, bool>> predicate)
		{
			return (IRavenQueryable<T>)Queryable.Where(source, predicate);
		}

		/// <summary>
		/// Filters a sequence of values based on a predicate.
		/// </summary>
		public static IRavenQueryable<T> Where<T>(this IRavenQueryable<T> source, Expression<Func<T, int, bool>> predicate)
		{
			return (IRavenQueryable<T>)Queryable.Where(source, predicate);
		}

		/// <summary>
		/// Sorts the elements of a sequence in ascending order according to a key.
		/// </summary>
		public static IRavenQueryable<T> OrderBy<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector)
		{
			return (IRavenQueryable<T>)Queryable.OrderBy(source, keySelector);
		}

		/// <summary>
		/// Sorts the elements of a sequence in ascending order according to a key.
		/// </summary>
		public static IRavenQueryable<T> OrderBy<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector, IComparer<TK> comparer)
		{
			return (IRavenQueryable<T>)Queryable.OrderBy(source, keySelector, comparer);
		}

		/// <summary>
		/// Sorts the elements of a sequence in descending order according to a key.
		/// </summary>
		public static IRavenQueryable<T> OrderByDescending<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector)
		{
			return (IRavenQueryable<T>)Queryable.OrderByDescending(source, keySelector);
		}

		/// <summary>
		/// Sorts the elements of a sequence in descending order according to a key.
		/// </summary>
		public static IRavenQueryable<T> OrderByDescending<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector, IComparer<TK> comparer)
		{
			return (IRavenQueryable<T>)Queryable.OrderByDescending(source, keySelector, comparer);
		}

		/// <summary>
		/// Projects each element of a sequence into a new form.
		/// </summary>
		public static IRavenQueryable<TResult> Select<TSource, TResult>(this IRavenQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
		{
			return (IRavenQueryable<TResult>)Queryable.Select(source, selector);
		}

		/// <summary>
		/// Projects each element of a sequence into a new form.
		/// </summary>
		public static IRavenQueryable<TResult> Select<TSource, TResult>(this IRavenQueryable<TSource> source, Expression<Func<TSource,int, TResult>> selector)
		{
			return (IRavenQueryable<TResult>)Queryable.Select(source, selector);
		}

		/// <summary>
		///  implementation of In operator
		/// </summary>
		public static bool In<T>(this T field, IEnumerable<T> values)
		{
			return values.Any(value => field.Equals(value));
		}


		/// <summary>
		///  implementation of In operator
		/// </summary>
		public static bool In<T>(this T field, params T[] values)
		{
			return values.Any(value => field.Equals(value));
		}

		/// <summary>
		/// Bypasses a specified number of elements in a sequence and then returns the remaining elements.
		/// </summary>
		/// Summary:
		public static IRavenQueryable<TSource> Skip<TSource>(this IRavenQueryable<TSource> source, int count)
		{
			return (IRavenQueryable<TSource>)Queryable.Skip(source,count);
		}
	}
}
