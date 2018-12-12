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
        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <param name="source">The source for querying</param>
        /// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
        /// <returns></returns>
        public static IRavenQueryable<TResult> Include<TResult>(this IQueryable<TResult> source, Expression<Func<TResult, object>> path)
        {
            return source.Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <typeparam name="TInclude">The type of the object that you want to include.</typeparam>
        /// <param name="source">The source for querying</param>
        /// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
        /// <returns></returns>
        public static IRavenQueryable<TResult> Include<TResult, TInclude>(this IQueryable<TResult> source, Expression<Func<TResult, object>> path)
        {
            var queryInspector = (IRavenQueryInspector)source;
            var conventions = queryInspector.Session.Conventions;

            return Include(source, IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(), conventions));
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <param name="source">The source for querying</param>
        /// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
        /// <returns></returns>
        public static IRavenQueryable<TResult> Include<TResult>(this IQueryable<TResult> source, string path)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(TResult));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path)));
            return (IRavenQueryable<TResult>)queryable;
        }

        /// <summary>
        /// Includes the specified documents and/or counters in the query
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <param name="source">The source for querying</param>
        /// <param name="includes">Specifies the documents and/or counters to include </param>
        /// <returns></returns>
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

        public static IAggregationQuery<T> AggregateBy<T>(this IQueryable<T> source, FacetBase facet)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            var query = new AggregationQuery<T>(source, ConvertExpressionIfNecessary, ConvertMethodIfNecessary, currentMethod);
            return query.AndAggregateBy(facet);
        }

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

        public static IAggregationQuery<T> AggregateBy<T>(this IQueryable<T> source, Action<IFacetBuilder<T>> builder)
        {
            var f = new FacetBuilder<T>();
            builder.Invoke(f);

            return source.AggregateBy(f.Facet);
        }

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

            var membersList = ReflectionUtil.GetPropertiesAndFieldsFor<TResult>(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).ToList();
            ravenQueryInspector.FieldsToFetch(membersList.Select(x => x.Name));
            return (IRavenQueryable<TResult>)results;
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static ISuggestionQuery<T> SuggestUsing<T>(this IQueryable<T> source, SuggestionBase suggestion)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            source = source.Provider.CreateQuery<T>(Expression.Call(null, currentMethod, expression, Expression.Constant(suggestion)));

            return new SuggestionQuery<T>(source, ConvertExpressionIfNecessary, ConvertMethodIfNecessary, currentMethod);
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static ISuggestionQuery<T> SuggestUsing<T>(this IQueryable<T> source, Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>();
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
        public static IRavenQueryable<T> Search<T>(this IQueryable<T> self, Expression<Func<T, object>> fieldSelector, string searchTerms,
                                                   decimal boost = 1,
                                                   SearchOptions options = SearchOptions.Guess)
        {
            var currentMethod = typeof(LinqExtensions).GetMethod(nameof(Search));

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(self);

            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod, expression,
                                                                      fieldSelector,
                                                                      Expression.Constant(searchTerms),
                                                                      Expression.Constant(boost),
                                                                      Expression.Constant(options)));
            return (IRavenQueryable<T>)queryable;
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

        public static IRavenQueryable<IGrouping<TKey, TSource>> GroupByArrayValues<TSource, TKey>(this IQueryable<TSource> source,
            Expression<Func<TSource, IEnumerable<TKey>>> fieldSelector)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, new[] { typeof(TSource), typeof(TKey) });
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, fieldSelector));

            return (IRavenQueryable<IGrouping<TKey, TSource>>)queryable;
        }

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
            return source.Spatial(path.ToPropertyPath(), clause);
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
            return source.Spatial(field(DynamicSpatialFieldFactory<T>.Instance), clause);
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
            return source.OrderByDistance(field(DynamicSpatialFieldFactory<T>.Instance), latitude, longitude);
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
            return source.OrderByDistance(path.ToPropertyPath(), latitude, longitude);
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, string fieldName, double latitude, double longitude)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(fieldName), Expression.Constant(latitude), Expression.Constant(longitude)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistance<T>(this IQueryable<T> source, Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt)
        {
            return source.OrderByDistance(field(DynamicSpatialFieldFactory<T>.Instance), shapeWkt);
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
            return source.OrderByDistance(path.ToPropertyPath(), shapeWkt);
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
            return source.OrderByDistanceDescending(field(DynamicSpatialFieldFactory<T>.Instance), latitude, longitude);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, DynamicSpatialField field, double latitude, double longitude)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(field), Expression.Constant(latitude), Expression.Constant(longitude)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Expression<Func<T, object>> path, double latitude, double longitude)
        {
            return source.OrderByDistanceDescending(path.ToPropertyPath(), latitude, longitude);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, string fieldName, double latitude, double longitude)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(fieldName), Expression.Constant(latitude), Expression.Constant(longitude)));
            return (IRavenQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt)
        {
            return source.OrderByDistanceDescending(field(DynamicSpatialFieldFactory<T>.Instance), shapeWkt);
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
            return source.OrderByDistanceDescending(path.ToPropertyPath(), shapeWkt);
        }

        public static IOrderedQueryable<T> OrderByDistanceDescending<T>(this IQueryable<T> source, string fieldName, string shapeWkt)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(fieldName), Expression.Constant(shapeWkt)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IOrderedQueryable<T> OrderBy<T>(this IQueryable<T> source, Expression<Func<T, object>> path, string sorterName)
        {
            return source.OrderBy(path.ToPropertyPath(), sorterName);
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
            return source.OrderBy(path.ToPropertyPath(), ordering);
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
            return source.OrderByDescending(path.ToPropertyPath(), sorterName);
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
            return source.OrderByDescending(path.ToPropertyPath(), ordering);
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
            return source.ThenBy(path.ToPropertyPath(), sorterName);
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
            return source.ThenBy(path.ToPropertyPath(), ordering);
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
            return source.ThenByDescending(path.ToPropertyPath(), sorterName);
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
            return source.ThenByDescending(path.ToPropertyPath(), ordering);
        }

        public static IOrderedQueryable<T> ThenByDescending<T>(this IOrderedQueryable<T> source, string path, OrderingType ordering = OrderingType.String)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(path), Expression.Constant(ordering)));
            return (IOrderedQueryable<T>)queryable;
        }

        public static IRavenQueryable<T> MoreLikeThis<T>(this IQueryable<T> source, MoreLikeThisBase moreLikeThis)
        {
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();

            currentMethod = ConvertMethodIfNecessary(currentMethod, typeof(T));
            var expression = ConvertExpressionIfNecessary(source);

            var queryable = source.Provider.CreateQuery(Expression.Call(null, currentMethod, expression, Expression.Constant(moreLikeThis)));
            return (IRavenQueryable<T>)queryable;
        }

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
