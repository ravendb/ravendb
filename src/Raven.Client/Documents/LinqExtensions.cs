//-----------------------------------------------------------------------
// <copyright file="LinqExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Extensions;
using Raven.Client.Util;

namespace Raven.Client.Documents
{
    ///<summary>
    /// Extensions to the linq syntax
    ///</summary>
    public static class LinqExtensions
    {

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(Expression{Func{T, string}})"/>
        /// <param name="source">The source for querying</param>
        public static IRavenQueryable<TResult> Include<TResult>(this IQueryable<TResult> source, Expression<Func<TResult, object>> path)
        {
            return source.Include(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions));
        }

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments{TInclude}(Expression{Func{T, string}})"/>
        /// <param name="source">The source for querying</param>
        public static IRavenQueryable<TResult> Include<TResult, TInclude>(this IQueryable<TResult> source, Expression<Func<TResult, object>> path)
        {
            var queryInspector = (IRavenQueryInspector)source;
            var conventions = queryInspector.Session.Conventions;

            return Include(source, IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), conventions));
        }

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}.IncludeDocuments(string)"/>
        /// <param name="source">The source for querying</param>
        public static IRavenQueryable<TResult> Include<TResult>(this IQueryable<TResult> source, string path)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(TResult));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path)));
            return (IRavenQueryable<TResult>)queryable;
        }
        
        /// <summary>
        /// Filter allows querying on documents without the need for issuing indexes. It is meant for exploratory queries or post query filtering. Criteria are evaluated at query time so please use Filter wisely to avoid performance issues.
        /// </summary>
        /// <typeparam name="T">The type of the object that holds querying type.</typeparam>
        /// <param name="source">The source for querying</param>
        /// <param name="predicate"></param>
        /// <param name="limit">Limits the number of documents processed by Filter.</param>
        /// <returns></returns>
        public static IRavenQueryable<T> Filter<T>(this IRavenQueryable<T> source, Expression<Func<T, bool>> predicate, int limit = int.MaxValue)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, predicate, Expression.Constant(limit)));
            
            return (IRavenQueryable<T>)queryable;
        }

        /// <summary>
        /// <inheritdoc cref="Filter{T}"/>
        /// </summary>
        public static IRavenQueryable<T> Filter<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            int limit = int.MaxValue)
        
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, predicate, Expression.Constant(limit)));
            return (IRavenQueryable<T>)queryable;
        }

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}"/>
        /// <param name="includes">Includes builder. Specifies the documents/counters/revisions/time series to load from the server. See more at: <see cref="IncludeBuilder"/></param>.
        /// <param name="source">The source for querying</param>
        public static IRavenQueryable<TResult> Include<TResult>(this IQueryable<TResult> source, Action<IQueryIncludeBuilder<TResult>> includes)
        {
            var queryInspector = (IRavenQueryInspector)source;
            var conventions = queryInspector.Session.Conventions;

            var includeBuilder = new IncludeBuilder<TResult>(conventions);
            includes.Invoke(includeBuilder);

            return Include(source, includeBuilder);
        }

        private static IRavenQueryable<TResult> Include<TResult>(this IQueryable<TResult> source, IncludeBuilder includes)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(TResult));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(includes)));

            return (IRavenQueryable<TResult>)queryable;
        }

        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(FacetBase)"/>
        public static IAggregationQuery<T> AggregateBy<T>(this IQueryable<T> source, FacetBase facet)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            var query = new AggregationQuery<T>(source, ConvertExpressionIfNecessary, ConvertMethodIfNecessary, currentMethod);
            return query.AndAggregateBy(facet);
        }

        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(FacetBase)"/>
        /// <param name="facets">List of aggregations</param>
        public static IAggregationQuery<T> AggregateBy<T>(this IQueryable<T> source, IEnumerable<FacetBase> facets)
        {
            IAggregationQuery<T> query = null;
            foreach (var facet in facets)
            {
                if (query == null)
                {
                    query = source.AggregateBy(facet);
                    continue;
                }

                query = query.AndAggregateBy(facet);
            }

            return query;
        }

        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(System.Action{Raven.Client.Documents.Queries.Facets.IFacetBuilder{T}})"/>
        public static IAggregationQuery<T> AggregateBy<T>(this IQueryable<T> source, Action<IFacetBuilder<T>> builder)
        {
            var f = new FacetBuilder<T>(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions);
            builder.Invoke(f);

            return source.AggregateBy(f.Facet);
        }

        /// <inheritdoc cref="IAggregationQuery{T}.AndAggregateBy(System.Action{Raven.Client.Documents.Queries.Facets.IFacetBuilder{T}})"/>
        /// <param name="facetSetupDocumentId">ID of the document where facet definition is stored. See more at <see cref="FacetSetup"/>.</param>
        public static IAggregationQuery<T> AggregateUsing<T>(this IQueryable<T> source, string facetSetupDocumentId)
        {
            if (facetSetupDocumentId == null)
                throw new ArgumentNullException(nameof(facetSetupDocumentId));

            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);
            source = source.Provider.CreateQuery<T>(Expression.Call(null, currentMethod, expression, Expression.Constant(facetSetupDocumentId)));

            return new AggregationQuery<T>(source, ConvertExpressionIfNecessary, ConvertMethodIfNecessary, currentMethod);
        }

        /// <summary>
        /// Project into a different type.
        /// </summary>
        public static IQueryable<TResult> As<TResult>(this IQueryable queryable)
        {
            var ofType = queryable.OfType<TResult>();
            var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
            return results;
        }

        /// <summary>
        /// Partition the query so we can intersect different parts of the query
        /// across different index entries.
        /// </summary>
        public static IRavenQueryable<T> Intersect<T>(this IQueryable<T> self)
        {
            var currentMethod = typeof(LinqExtensions).GetMethod("Intersect");

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(self);

            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod, expression));

            return (IRavenQueryable<T>)queryable;
        }

        /// <summary>
        /// Project query results according to the specified type
        /// </summary>
        public static IRavenQueryable<TResult> ProjectInto<TResult>(this IQueryable queryable)
        {
            var ofType = queryable.OfType<TResult>();
            var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
            var ravenQueryInspector = (RavenQueryInspector<TResult>)results;

            var membersList = ReflectionUtil.GetPropertiesAndFieldsFor<TResult>(ReflectionUtil.BindingFlagsConstants.QueryingFields).ToList();
            
            ravenQueryInspector.FieldsToFetch(membersList);
            return (IRavenQueryable<TResult>)results;
        }

        /// <inheritdoc cref="ISuggestionQuery{T}.AndSuggestUsing(SuggestionBase)"/>
        public static ISuggestionQuery<T> SuggestUsing<T>(this IQueryable<T> source, SuggestionBase suggestion)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            source = source.Provider.CreateQuery<T>(Expression.Call(null, currentMethod, expression, Expression.Constant(suggestion)));

            return new SuggestionQuery<T>(source, ConvertExpressionIfNecessary, ConvertMethodIfNecessary, currentMethod);
        }

        /// <inheritdoc cref="ISuggestionQuery{T}.AndSuggestUsing(Action{ISuggestionBuilder{T}})"/>
        public static ISuggestionQuery<T> SuggestUsing<T>(this IQueryable<T> source, Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions);
            builder?.Invoke(f);

            return source.SuggestUsing(f.Suggestion);
        }

        /// <summary>
        /// Register the query as a lazy async query in the session and return a lazy async
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<Task<IEnumerable<T>>> LazilyAsync<T>(this IQueryable<T> source)
        {
            return LazilyAsync(source, null);
        }

        /// <summary>
        /// Register the query as a lazy async query in the session and return a lazy async
        /// instance that will evaluate the query only when needed
        /// As well as a function to execute when the value is evaluated
        /// </summary>

        public static Lazy<Task<IEnumerable<T>>> LazilyAsync<T>(this IQueryable<T> source, Action<IEnumerable<T>> onEval)
        {
            var provider = source.Provider as IRavenQueryProvider;
            if (provider == null)
                throw new ArgumentException("You can only use Raven Queryable with Lazily");

            return provider.LazilyAsync(source.Expression, onEval);
        }

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
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<int> CountLazily<T>(this IQueryable<T> source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountLazily only be used with IRavenQueryable");

            return provider.CountLazily<T>(source.Expression);
        }

        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<long> LongCountLazily<T>(this IQueryable<T> source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("LongCountLazily only be used with IRavenQueryable");

            return provider.LongCountLazily<T>(source.Expression);
        }

        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<Task<int>> CountLazilyAsync<T>(this IQueryable<T> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountLazily only be used with IRavenQueryable");

            return provider.CountLazilyAsync<T>(source.Expression, token);
        }

        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<Task<long>> LongCountLazilyAsync<T>(this IQueryable<T> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountLazily only be used with IRavenQueryable");

            return provider.LongCountLazilyAsync<T>(source.Expression, token);
        }

        /// <summary>
        /// Returns a list of results for a query asynchronously. 
        /// </summary>
        public static Task<List<T>> ToListAsync<T>(this IQueryable<T> source, CancellationToken token = default)
        {
            var provider = source.Provider as IRavenQueryProvider;
            if (provider == null)
                throw new ArgumentException("You can only use Raven Queryable with ToListAsync");

            var documentQuery = provider.ToAsyncDocumentQuery<T>(source.Expression);
            return documentQuery.ToListAsync(token);
        }

        /// <summary>
        /// Returns an array of results for a query asynchronously. 
        /// </summary>
        public static Task<T[]> ToArrayAsync<T>(this IQueryable<T> source, CancellationToken token = default)
        {
            var provider = source.Provider as IRavenQueryProvider;
            if (provider == null)
                throw new ArgumentException("You can only use Raven Queryable with ToArrayAsync");

            var documentQuery = provider.ToAsyncDocumentQuery<T>(source.Expression);
            return documentQuery.ToArrayAsync(token);
        }

        /// <summary>
        /// Determines whether a sequence contains any elements.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// true if the source sequence contains any elements; otherwise, false.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        public static Task<bool> AnyAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("AnyAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression);

            return query.AnyAsync(token);
        }

        /// <summary>
        /// Determines whether any element of a sequence satisfies a condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// 
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// true if any elements in the source sequence pass the test in the specified
        /// predicate; otherwise, false.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        public static Task<bool> AnyAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("AnyAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression);

            return query.AnyAsync(token);
        }

        /// <summary>
        /// Returns the number of elements in a sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The number of elements in the input sequence.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="OverflowException">
        /// The number of elements in source is larger than <see cref="Int32.MaxValue"/>.
        /// </exception>
        public static Task<int> CountAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression);

            return query.CountAsync(token);
        }

        /// <summary>
        /// Returns the number of elements in a sequence as int64 type.
        /// </summary>
        public static long LongCount<TSource>(this IQueryable<TSource> source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException(nameof(provider));

            var query = provider.ToDocumentQuery<TSource>(source.Expression);
            return query.LongCount();
        }

        /// <summary>
        /// Returns the number of elements in a sequence as int64 type.
        /// </summary>
        public static Task<long> LongCountAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException(nameof(provider));

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression);
            return query.LongCountAsync(token);
        }

        /// <summary>
        /// Returns the number of elements in the specified sequence that satisfies a condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// 
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The number of elements in the sequence that satisfies the condition in
        /// the predicate function.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="OverflowException">
        /// The number of elements in source is larger than <see cref="Int32.MaxValue"/>.
        /// </exception>
        public static Task<int> CountAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression);

            return query.CountAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The first element in source.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// The source sequence is empty or source
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression);

            return query.FirstAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence that satisfies a specified condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The first element in source.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// No element satisfies the condition in predicate,
        /// the source sequence is empty or source
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression);

            return query.FirstAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence, or a default value if the sequence contains no elements.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// default(TSource) if source is empty; otherwise,
        /// the first element in source.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> FirstOrDefaultAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression);

            return query.FirstOrDefaultAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence that satisfies a specified
        /// condition or a default value if no such element is found.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// default(TSource) if source is empty or
        /// if no element passes the test specified by predicate;
        /// otherwise, the first element in source that passes
        /// the test specified by predicate.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> FirstOrDefaultAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression);

            return query.FirstOrDefaultAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence, and throws an exception if there
        /// is not exactly one element in the sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the single element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// The source sequence is empty, has more than one element or
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression);

            return query.SingleAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence, and throws an exception if there
        /// is not exactly one element in the sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the single element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence that satisfies the condition in predicate.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// No element satisfies the condition in predicate, more than
        /// one element satisfies the condition, the source sequence is empty or
        /// source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression);

            return query.SingleAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence, or a default value if the
        /// sequence is empty; this method throws an exception if there is more than one
        /// element in the sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence, or default(TSource)
        /// if the sequence contains no elements.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// source has more than one element or
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> SingleOrDefaultAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression);

            return query.SingleOrDefaultAsync(token);
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence that satisfies a specified
        /// condition or a default value if no such element exists; this method throws an
        /// exception if more than one element satisfies the condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// 
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence that satisfies the condition in predicate,
        /// or default(TSource) if no such element is found.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// More than one element satisfies the condition in predicate
        /// or source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static Task<TSource> SingleOrDefaultAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression);

            return query.SingleOrDefaultAsync(token);
        }

        /// <summary>        
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.        
        /// </summary>
        /// <typeparam name="T">The type of element of self</typeparam>
        /// <param name="self">The <see cref="IQueryable{T}"/> to search on</param>
        /// <param name="fieldSelector">Function returning the field to search on</param>
        /// <param name="searchTerms">Field terms to search for, separated with whitespaces</param>
        /// <param name="boost">Boost factor for sorting purposes</param>
        /// <param name="options">Logical operator to use in relation to the previous filtering statement.</param>
        /// <param name="operator">Determines the logical operator between all of the terms received in searchTerms parameter</param>
        public static IRavenQueryable<T> Search<T>(this IQueryable<T> self, Expression<Func<T, object>> fieldSelector, string searchTerms,
                                                   decimal boost = 1,
                                                   SearchOptions options = SearchOptions.Guess,
                                                   SearchOperator @operator = SearchOperator.Or)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(self);

            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod, expression,
                                                                      fieldSelector,
                                                                      Expression.Constant(searchTerms),
                                                                      Expression.Constant(boost),
                                                                      Expression.Constant(options),
                                                                      Expression.Constant(@operator)));
            return (IRavenQueryable<T>)queryable;
        }

        /// <summary>        
        /// Perform a search for documents which fields match the searchTerms array.
        /// Each array element will be checked independently.        
        /// </summary>
        /// <typeparam name="T">The type of element of self</typeparam>
        /// <param name="self">The <see cref="IQueryable{T}"/> to search on</param>
        /// <param name="fieldSelector">Function returning the field to search on</param>
        /// <param name="searchTerms">Array of terms to search for.</param>
        /// <param name="boost">Boost factor for sorting purposes</param>
        /// <param name="options">Logical operator to use in relation to the previous filtering statement.</param>
        /// <param name="operator">Determines the logical operator between all of the terms received in searchTerms parameter</param>
        public static IRavenQueryable<T> Search<T>(this IQueryable<T> self, Expression<Func<T, object>> fieldSelector, IEnumerable<string> searchTerms,
            decimal boost = 1,
            SearchOptions options = SearchOptions.Guess,
            SearchOperator @operator = SearchOperator.Or)
        {
            var termToSearch = string.Join(" ", searchTerms);

            if (string.IsNullOrEmpty(termToSearch))
                throw new ArgumentException($"Please add search terms. Cannot search on empty {nameof(searchTerms)} array.");

            return Search(self, fieldSelector, termToSearch, boost, options, @operator);
        }

        /// <summary>
        /// Perform an initial sort by lucene score.
        /// </summary>
        public static IOrderedQueryable<T> OrderByScore<T>(this IQueryable<T> self)
        {
            var currentMethod = typeof(LinqExtensions).GetMethod(nameof(OrderByScore));

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(self);

            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod, expression));
            return (IOrderedQueryable<T>)queryable;
        }

        /// <summary>
        /// Perform an initial sort by lucene score.
        /// </summary>
        public static IOrderedQueryable<T> ThenByScore<T>(this IOrderedQueryable<T> self)
        {
            var currentMethod = typeof(LinqExtensions).GetMethod(nameof(ThenByScore));

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(self);

            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod, expression));
            return (IOrderedQueryable<T>)queryable;
        }

        /// <summary>
        /// Perform an initial sort by lucene score descending.
        /// </summary>
        public static IOrderedQueryable<T> OrderByScoreDescending<T>(this IQueryable<T> self)
        {
            var currentMethod = typeof(LinqExtensions).GetMethod(nameof(OrderByScoreDescending));

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(self);

            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod, expression));
            return (IOrderedQueryable<T>)queryable;
        }

        /// <summary>
        /// Perform an initial sort by lucene score descending.
        /// </summary>
        public static IOrderedQueryable<T> ThenByScoreDescending<T>(this IOrderedQueryable<T> self)
        {
            var currentMethod = typeof(LinqExtensions).GetMethod(nameof(ThenByScoreDescending));

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(self);

            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod, expression));
            return (IOrderedQueryable<T>)queryable;
        }

        /// <summary>
        /// Returns the query results as a stream
        /// </summary>
        public static void ToStream<T>(this IQueryable<T> self, Stream stream)
        {
            var queryProvider = (IRavenQueryProvider)self.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(self.Expression);
            ToStream(docQuery, stream);
        }
        /// <summary> 
        /// Returns the query results as a stream
        /// </summary>
        public static void ToStream<T>(this IDocumentQuery<T> self, Stream stream)
        {
            var documentQuery = (DocumentQuery<T>)self;
            var session = (DocumentSession)documentQuery.Session;
            session.Advanced.StreamInto(self, stream);
        }

        /// <summary>
        /// Returns the query results as a stream
        /// </summary>
        public static async Task ToStreamAsync<T>(this IQueryable<T> self, Stream stream, CancellationToken token = default)
        {
            var queryProvider = (IRavenQueryProvider)self.Provider;
            var docQuery = queryProvider.ToAsyncDocumentQuery<T>(self.Expression);
            await ToStreamAsync(docQuery, stream, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Returns the query results as a stream
        /// </summary>
        public static async Task ToStreamAsync<T>(this IAsyncDocumentQuery<T> self, Stream stream, CancellationToken token = default)
        {
            var documentQuery = (AbstractDocumentQuery<T, AsyncDocumentQuery<T>>)self;
            var session = documentQuery.AsyncSession;
            await session.Advanced.StreamIntoAsync(self, stream, token).ConfigureAwait(false);
        }

        /// <summary>
        /// Creates a dynamic query which does an aggregation of data grouped by individual values of an array. Underneath a fanout auto map-reduce index will be created to handle such query.
        /// </summary>
        /// <param name="fieldSelector">Path of the array</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.GroupByArrayQuery"/>
        public static IRavenQueryable<IGrouping<TKey, TSource>> GroupByArrayValues<TSource, TKey>(this IQueryable<TSource> source,
            Expression<Func<TSource, IEnumerable<TKey>>> fieldSelector)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, new[] { typeof(TSource), typeof(TKey) });
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, fieldSelector));

            return (IRavenQueryable<IGrouping<TKey, TSource>>)queryable;
        }

        /// <summary>
        /// Creates a dynamic query which does an aggregation of data grouped by entire content of an array. The group by key will be calculated by hashing all values of the array.
        /// Underneath an auto map-reduce index will be created to handle such query.
        /// </summary>
        /// <param name="fieldSelector">Path of the array</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.GroupByArrayContent"/>
        public static IRavenQueryable<IGrouping<IEnumerable<TKey>, TSource>> GroupByArrayContent<TSource, TKey>(this IQueryable<TSource> source,
            Expression<Func<TSource, IEnumerable<TKey>>> fieldSelector)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, new[] { typeof(TSource), typeof(TKey) });
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, fieldSelector));

            return (IRavenQueryable<IGrouping<IEnumerable<TKey>, TSource>>)queryable;
        }

        public static IRavenQueryable<T> Where<T>(this IQueryable<T> source, Expression<Func<T, int, bool>> predicate, bool exact)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, predicate, Expression.Constant(exact)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IRavenQueryable<T> Where<T>(this IQueryable<T> source, Expression<Func<T, bool>> predicate, bool exact)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, predicate, Expression.Constant(exact)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IRavenQueryable<T> Spatial<T>(this IQueryable<T> source, Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            return source.Spatial(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), clause);
        }

        public static IRavenQueryable<T> Spatial<T>(this IQueryable<T> source, string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(fieldName), Expression.Constant(clause)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IRavenQueryable<T> Spatial<T>(this IQueryable<T> source, Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            return source.Spatial(field(new DynamicSpatialFieldFactory<T>(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions)), clause);
        }

        public static IRavenQueryable<T> Spatial<T>(this IQueryable<T> source, DynamicSpatialField field, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(field), Expression.Constant(clause)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude)
        {
            return source.OrderByDistance(field(new DynamicSpatialFieldFactory<T>(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions)), latitude, longitude);
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, DynamicSpatialField field, double latitude, double longitude)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(field), Expression.Constant(latitude), Expression.Constant(longitude)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, Expression<Func<T, object>> path, double latitude, double longitude)
        {
            return OrderByDistance<T>(source, path, latitude, longitude, 0);
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, Expression<Func<T, object>> path, double latitude, double longitude, double roundFactor)
        {
            return source.OrderByDistance(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), latitude, longitude, roundFactor);
        }
        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, string fieldName, double latitude, double longitude)
        {
            return OrderByDistance<T>(source, fieldName, latitude, longitude, 0);
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, string fieldName, double latitude, double longitude, double roundFactor)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(
                Expression.Call(null, currentMethod, expression, 
                    Expression.Constant(fieldName), 
                    Expression.Constant(latitude), 
                    Expression.Constant(longitude), 
                    Expression.Constant(roundFactor)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt)
        {
            return source.OrderByDistance(field(new DynamicSpatialFieldFactory<T>(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions)), shapeWkt);
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, DynamicSpatialField field, string shapeWkt)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(field), Expression.Constant(shapeWkt)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, Expression<Func<T, object>> path, string shapeWkt)
        {
            return source.OrderByDistance(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), shapeWkt);
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, string fieldName, string shapeWkt)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(fieldName), Expression.Constant(shapeWkt)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude)
        {
            return source.OrderByDistanceDescending(field(new DynamicSpatialFieldFactory<T>(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions)), latitude, longitude);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, DynamicSpatialField field, double latitude, double longitude)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, 
                    Expression.Constant(field), 
                    Expression.Constant(latitude), 
                    Expression.Constant(longitude)));
            return (IOrderedQueryable<T>)queryable;
        }
        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Expression<Func<T, object>> path, double latitude, double longitude)
        {
            return OrderByDistanceDescending(source, path, latitude, longitude, 0);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Expression<Func<T, object>> path, double latitude, double longitude, double roundFactor)
        {
            return source.OrderByDistanceDescending(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), latitude, longitude, roundFactor);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, string fieldName, double latitude, double longitude)
        {
            return OrderByDistanceDescending(source, fieldName, latitude, longitude, 0);
        }
        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, string fieldName, double latitude, double longitude, double roundFactor)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(
                Expression.Call(null, currentMethod, expression, 
                Expression.Constant(fieldName), 
                Expression.Constant(latitude), 
                Expression.Constant(longitude),
                Expression.Constant(roundFactor)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt)
        {
            return source.OrderByDistanceDescending(field(new DynamicSpatialFieldFactory<T>(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions)), shapeWkt);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, DynamicSpatialField field, string shapeWkt)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(field), Expression.Constant(shapeWkt)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Expression<Func<T, object>> path, string shapeWkt)
        {
            return OrderByDistanceDescending(source, path, shapeWkt, 0);
        }
        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Expression<Func<T, object>> path, string shapeWkt, double roundFactor)
        {
            return source.OrderByDistanceDescending(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), shapeWkt, roundFactor);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, string fieldName, string shapeWkt)
        {
            return OrderByDistanceDescending<T>(source, fieldName, shapeWkt, 0);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, string fieldName, string shapeWkt, double roundFactor)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(
                Expression.Call(null, currentMethod, expression, 
                    Expression.Constant(fieldName), 
                    Expression.Constant(shapeWkt),
                    Expression.Constant(roundFactor)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderBy<T>(this IQueryable<T> source, Expression<Func<T, object>> path, string sorterName)
        {
            return source.OrderBy(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), sorterName);
        }

        public static IOrderedQueryable<T> OrderBy<T>(this IQueryable<T> source, string path, string sorterName)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(sorterName)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderBy<T>(this IQueryable<T> source, Expression<Func<T, object>> path, OrderingType ordering = OrderingType.String)
        {
            return source.OrderBy(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), ordering);
        }

        public static IOrderedQueryable<T> OrderBy<T>(this IQueryable<T> source, string path, OrderingType ordering = OrderingType.String)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(ordering)));
            return (IOrderedQueryable<T>)queryable;

        }

        public static IOrderedQueryable<T> OrderByDescending<T>(this IQueryable<T> source, Expression<Func<T, object>> path, string sorterName)
        {
            return source.OrderByDescending(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), sorterName);
        }

        public static IOrderedQueryable<T> OrderByDescending<T>(this IQueryable<T> source, string path, string sorterName)
        {
            if (string.IsNullOrWhiteSpace(sorterName))
                throw new ArgumentNullException(nameof(sorterName));

            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(sorterName)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDescending<T>(this IQueryable<T> source, Expression<Func<T, object>> path, OrderingType ordering = OrderingType.String)
        {
            return source.OrderByDescending(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), ordering);
        }

        public static IOrderedQueryable<T> OrderByDescending<T>(this IQueryable<T> source, string path, OrderingType ordering = OrderingType.String)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(ordering)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> ThenBy<T>(this IOrderedQueryable<T> source, Expression<Func<T, object>> path, string sorterName)
        {
            return source.ThenBy(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), sorterName);
        }

        public static IOrderedQueryable<T> ThenBy<T>(this IOrderedQueryable<T> source, string path, string sorterName)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(sorterName)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> ThenBy<T>(this IOrderedQueryable<T> source, Expression<Func<T, object>> path, OrderingType ordering = OrderingType.String)
        {
            return source.ThenBy(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), ordering);
        }

        public static IOrderedQueryable<T> ThenBy<T>(this IOrderedQueryable<T> source, string path, OrderingType ordering = OrderingType.String)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(ordering)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> ThenByDescending<T>(this IOrderedQueryable<T> source, Expression<Func<T, object>> path, string sorterName)
        {
            return source.ThenByDescending(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), sorterName);
        }

        public static IOrderedQueryable<T> ThenByDescending<T>(this IOrderedQueryable<T> source, string path, string sorterName)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(sorterName)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> ThenByDescending<T>(this IOrderedQueryable<T> source, Expression<Func<T, object>> path, OrderingType ordering = OrderingType.String)
        {
            return source.ThenByDescending(path.ToPropertyPath(((IRavenQueryProvider)source.Provider).QueryGenerator.Conventions), ordering);
        }

        public static IOrderedQueryable<T> ThenByDescending<T>(this IOrderedQueryable<T> source, string path, OrderingType ordering = OrderingType.String)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(ordering)));
            return (IOrderedQueryable<T>)queryable;
        }

        /// <inheritdoc cref="MoreLikeThisBase" />
        /// <param name="moreLikeThis">Configure MoreLikeThis. You can use: <see cref="MoreLikeThisUsingDocumentForQuery{T}"/>, <see cref="MoreLikeThisUsingDocumentForDocumentQuery{T}"/>, <see cref="MoreLikeThisUsingAnyDocument"/> or <see cref="MoreLikeThisUsingDocument"/>.</param>
        public static IRavenQueryable<T> MoreLikeThis<T>(this IQueryable<T> source, MoreLikeThisBase moreLikeThis)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(moreLikeThis)));
            return (IRavenQueryable<T>)queryable;
        }

        /// <inheritdoc cref="IMoreLikeThisBuilder{T}" />
        /// <param name="builder">Configure MoreLikeThis by builder.</param>
        public static IRavenQueryable<T> MoreLikeThis<T>(this IQueryable<T> source, Action<IMoreLikeThisBuilder<T>> builder)
        {
            var f = new MoreLikeThisBuilder<T>();
            builder.Invoke(f);

            return source.MoreLikeThis(f.MoreLikeThis);
        }

        public static IDocumentQuery<T> ToDocumentQuery<T>(this IQueryable<T> source)
        {
            var expression = ConvertExpressionIfNecessary(source);

            var results = source.Provider.CreateQuery<T>(expression);

            var ravenQueryInspector = (RavenQueryInspector<T>)results;

            return ravenQueryInspector.GetDocumentQuery();
        }

        public static IAsyncDocumentQuery<T> ToAsyncDocumentQuery<T>(this IQueryable<T> source)
        {
            var expression = ConvertExpressionIfNecessary(source);

            var results = source.Provider.CreateQuery<T>(expression);

            var ravenQueryInspector = (RavenQueryInspector<T>)results;

            return ravenQueryInspector.GetAsyncDocumentQuery();
        }

        public static IRavenQueryable<T> Skip<T>(this IQueryable<T> source, long count)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(count)));
            return (IRavenQueryable<T>)queryable;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Expression ConvertExpressionIfNecessary<T>(IQueryable<T> source)
        {
            var expression = source.Expression;
            if (expression.Type != typeof(IRavenQueryable<T>))
                expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));

            return expression;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static MethodInfo ConvertMethodIfNecessary(MethodInfo method, Type typeArgument)
        {
            return method.IsGenericMethodDefinition ? method.MakeGenericMethod(typeArgument) : method;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static MethodInfo ConvertMethodIfNecessary(MethodInfo method, Type[] typeArguments)
        {
            return method.IsGenericMethodDefinition ? method.MakeGenericMethod(typeArguments) : method;
        }
    }
}
