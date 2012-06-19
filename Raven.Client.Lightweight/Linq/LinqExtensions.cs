//-----------------------------------------------------------------------
// <copyright file="LinqExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.Batches;

namespace Raven.Client.Linq
{
#if !NET35
	using System.Threading.Tasks;
#endif

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
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			var query = ravenQueryInspector.ToString();

			return ravenQueryInspector.DatabaseCommands.GetFacets(ravenQueryInspector.IndexQueried, new IndexQuery { Query = query }, facetDoc);
		}


#endif

#if !NET35 && !SILVERLIGHT
		public static Lazy<IDictionary<string, IEnumerable<FacetValue>>> ToFacetsLazy<T>(this IQueryable<T> queryable, string facetDoc)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			var query = ravenQueryInspector.ToString();

			var lazyOperation = new LazyFacetsOperation(ravenQueryInspector.IndexQueried, facetDoc, new IndexQuery { Query = query });

			var documentSession = ((DocumentSession)ravenQueryInspector.Session);
			return documentSession.AddLazyOperation<IDictionary<string, IEnumerable<FacetValue>>>(lazyOperation, null);
		}
#endif

#if !NET35



		/// <summary>
		/// Query the facets results for this query using the specified facet document
		/// </summary>
		public static Task<IDictionary<string, IEnumerable<FacetValue>>> ToFacetsAsync<T>(this IQueryable<T> queryable, string facetDoc)
		{
			var ravenQueryInspector = ((RavenQueryInspector<T>)queryable);
			var query = ravenQueryInspector.ToString();

			return ravenQueryInspector.AsyncDatabaseCommands.GetFacetsAsync(ravenQueryInspector.IndexQueried, new IndexQuery { Query = query }, facetDoc);
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
			return results;
		}

		/// <summary>
		/// Partition the query so we can intersect different parts of the query
		/// across different index entries.
		/// </summary>
		public static IRavenQueryable<T> Intersect<T>(this IQueryable<T> self)
		{
			var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
			Expression expression = self.Expression;
			if (expression.Type != typeof(IRavenQueryable<T>))
			{
				expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
			}
			var queryable =
				self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression));
			return (IRavenQueryable<T>)queryable;

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
			SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query);
			return ravenQueryInspector.DatabaseCommands.Suggest(ravenQueryInspector.IndexQueried, query);
		}
#if !NET35
		/// <summary>
		/// Lazy Suggest alternative values for the queried term
		/// </summary>
		public static Lazy<SuggestionQueryResult> SuggestLazy(this IQueryable queryable)
		{
			return SuggestLazy(queryable, new SuggestionQuery());
		}

		/// <summary>
		/// Lazy Suggest alternative values for the queried term
		/// </summary>
		public static Lazy<SuggestionQueryResult> SuggestLazy(this IQueryable queryable, SuggestionQuery query)
		{
			var ravenQueryInspector = ((IRavenQueryInspector)queryable);
			SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query);

			var lazyOperation = new LazySuggestOperation(ravenQueryInspector.IndexQueried, query);

			var documentSession = ((DocumentSession)ravenQueryInspector.Session);
			return documentSession.AddLazyOperation<SuggestionQueryResult>(lazyOperation, null);
		}
#endif

#endif

		private static void SetSuggestionQueryFieldAndTerm(IRavenQueryInspector queryInspector, SuggestionQuery query)
		{
			var lastEqualityTerm = queryInspector.GetLastEqualityTerm();
			if (lastEqualityTerm.Key == null)
				throw new InvalidOperationException("Could not suggest on a query that doesn't have a single equality check");

			query.Field = lastEqualityTerm.Key;
			query.Term = lastEqualityTerm.Value;
		}
#if !NET35
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
		public static IRavenQueryable<T> Search<T>(this IRavenQueryable<T> self, Expression<Func<T, object>> fieldSelector, string searchTerms, decimal boost = 1, SearchOptions options = SearchOptions.Or, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.EscapeAll)
		{
			var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
			Expression expression = self.Expression;
			if (expression.Type != typeof(IRavenQueryable<T>))
			{
				expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
			}
			var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression,
																	  fieldSelector,
																	  Expression.Constant(searchTerms),
																	  Expression.Constant(boost),
																	  Expression.Constant(options),
																	  Expression.Constant(escapeQueryOptions)));
			return (IRavenQueryable<T>)queryable;
		}

#if !NET35
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
			if (provider == null)
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
		/// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
		/// <param name="source">The source for querying</param>
		/// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
		/// <returns></returns>
		public static IRavenQueryable<TResult> Include<TResult>(this IRavenQueryable<TResult> source, Expression<Func<TResult, object>> path)
		{
			source.Customize(x => x.Include(path));
			return source;
		}

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
		/// <typeparam name="TInclude">The type of the object that you want to include.</typeparam>
		/// <param name="source">The source for querying</param>
		/// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
		/// <returns></returns>
		public static IRavenQueryable<TResult> Include<TResult, TInclude>(this IRavenQueryable<TResult> source, Expression<Func<TResult, object>> path)
		{
			source.Customize(x => x.Include<TResult, TInclude>(path));
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
		public static IRavenQueryable<TResult> Select<TSource, TResult>(this IRavenQueryable<TSource> source, Expression<Func<TSource, int, TResult>> selector)
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
			return (IRavenQueryable<TSource>)Queryable.Skip(source, count);
		}
	}
}