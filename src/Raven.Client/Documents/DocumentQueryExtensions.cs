#if NETSTANDARD2_0 || NETCOREAPP
#define CURRENT
#endif

#if NETSTANDARD1_3
#define LEGACY
#endif

using System.Collections.Generic;
using Raven.Client.Documents.Session;

namespace System.Linq
{
    public static class DocumentQueryExtensions
    {
        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TCollection, TResult>(this IDocumentQuery<TSource> source, Func<TSource, int, IEnumerable<TCollection>> collectionSelector, Func<TSource, TCollection, TResult> resultSelector)
        {
            return Enumerable.SelectMany<TSource, TCollection, TResult>(source, collectionSelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TCollection, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, int, IEnumerable<TCollection>> collectionSelector, Func<TSource, TCollection, TResult> resultSelector)
        {
            return Enumerable.SelectMany<TSource, TCollection, TResult>(source, collectionSelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TCollection, TResult>(this IDocumentQuery<TSource> source, Func<TSource, IEnumerable<TCollection>> collectionSelector, Func<TSource, TCollection, TResult> resultSelector)
        {
            return Enumerable.SelectMany<TSource, TCollection, TResult>(source, collectionSelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TCollection, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, IEnumerable<TCollection>> collectionSelector, Func<TSource, TCollection, TResult> resultSelector)
        {
            return Enumerable.SelectMany<TSource, TCollection, TResult>(source, collectionSelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean SequenceEqual<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.SequenceEqual<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean SequenceEqual<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.SequenceEqual<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean SequenceEqual<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.SequenceEqual<TSource>(first, second, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean SequenceEqual<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.SequenceEqual<TSource>(first, second, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Single<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Single<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Single<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Single<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Single<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Single<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Single<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Single<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource SingleOrDefault<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.SingleOrDefault<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource SingleOrDefault<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.SingleOrDefault<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource SingleOrDefault<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.SingleOrDefault<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource SingleOrDefault<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.SingleOrDefault<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Skip<TSource>(this IDocumentQuery<TSource> source, int count)
        {
            return Enumerable.Skip<TSource>(source, count);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Skip<TSource>(this IRawDocumentQuery<TSource> source, int count)
        {
            return Enumerable.Skip<TSource>(source, count);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> SkipWhile<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.SkipWhile<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> SkipWhile<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.SkipWhile<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> SkipWhile<TSource>(this IDocumentQuery<TSource> source, Func<TSource, int, System.Boolean> predicate)
        {
            return Enumerable.SkipWhile<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> SkipWhile<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, int, System.Boolean> predicate)
        {
            return Enumerable.SkipWhile<TSource>(source, predicate);
        }

        //[Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        //public static IEnumerable<TSource> SkipLast<TSource>(this IDocumentQuery<TSource> source, int count)
        //{
        //    return Enumerable.SkipLast<TSource>(source, count);
        //}

        //[Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        //public static IEnumerable<TSource> SkipLast<TSource>(this IRawDocumentQuery<TSource> source, int count)
        //{
        //    return Enumerable.SkipLast<TSource>(source, count);
        //}

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Sum(this IDocumentQuery<int> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Sum(this IRawDocumentQuery<int> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Sum(this IDocumentQuery<Int32?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Sum(this IRawDocumentQuery<Int32?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Sum(this IDocumentQuery<System.Int64> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Sum(this IRawDocumentQuery<System.Int64> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Sum(this IDocumentQuery<Int64?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Sum(this IRawDocumentQuery<Int64?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Sum(this IDocumentQuery<System.Single> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Sum(this IRawDocumentQuery<System.Single> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Sum(this IDocumentQuery<Single?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Sum(this IRawDocumentQuery<Single?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Sum(this IDocumentQuery<System.Double> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Sum(this IRawDocumentQuery<System.Double> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Sum(this IDocumentQuery<Double?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Sum(this IRawDocumentQuery<Double?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Sum(this IDocumentQuery<decimal> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Sum(this IRawDocumentQuery<decimal> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Sum(this IDocumentQuery<Decimal?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Sum(this IRawDocumentQuery<Decimal?> source)
        {
            return Enumerable.Sum(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Sum<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Sum<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Sum<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Take<TSource>(this IDocumentQuery<TSource> source, int count)
        {
            return Enumerable.Take<TSource>(source, count);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Take<TSource>(this IRawDocumentQuery<TSource> source, int count)
        {
            return Enumerable.Take<TSource>(source, count);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> TakeWhile<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.TakeWhile<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> TakeWhile<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.TakeWhile<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> TakeWhile<TSource>(this IDocumentQuery<TSource> source, Func<TSource, int, System.Boolean> predicate)
        {
            return Enumerable.TakeWhile<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> TakeWhile<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, int, System.Boolean> predicate)
        {
            return Enumerable.TakeWhile<TSource>(source, predicate);
        }

        //[Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        //public static IEnumerable<TSource> TakeLast<TSource>(this IDocumentQuery<TSource> source, int count)
        //{
        //    return Enumerable.TakeLast<TSource>(source, count);
        //}

        //[Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        //public static IEnumerable<TSource> TakeLast<TSource>(this IRawDocumentQuery<TSource> source, int count)
        //{
        //    return Enumerable.TakeLast<TSource>(source, count);
        //}

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Union<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Union<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Union<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Union<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Union<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Union<TSource>(first, second, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Union<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Union<TSource>(first, second, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Where<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Where<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Where<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Where<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Where<TSource>(this IDocumentQuery<TSource> source, Func<TSource, int, System.Boolean> predicate)
        {
            return Enumerable.Where<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Where<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, int, System.Boolean> predicate)
        {
            return Enumerable.Where<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Zip<TFirst, TSecond, TResult>(this IDocumentQuery<TFirst> first, IEnumerable<TSecond> second, Func<TFirst, TSecond, TResult> resultSelector)
        {
            return Enumerable.Zip<TFirst, TSecond, TResult>(first, second, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Zip<TFirst, TSecond, TResult>(this IRawDocumentQuery<TFirst> first, IEnumerable<TSecond> second, Func<TFirst, TSecond, TResult> resultSelector)
        {
            return Enumerable.Zip<TFirst, TSecond, TResult>(first, second, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Join<TOuter, TInner, TKey, TResult>(this IDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, TInner, TResult> resultSelector)
        {
            return Enumerable.Join<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Join<TOuter, TInner, TKey, TResult>(this IRawDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, TInner, TResult> resultSelector)
        {
            return Enumerable.Join<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Join<TOuter, TInner, TKey, TResult>(this IDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, TInner, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.Join<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Join<TOuter, TInner, TKey, TResult>(this IRawDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, TInner, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.Join<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Last<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Last<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Last<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Last<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Last<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Last<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Last<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Last<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource LastOrDefault<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.LastOrDefault<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource LastOrDefault<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.LastOrDefault<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource LastOrDefault<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.LastOrDefault<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource LastOrDefault<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.LastOrDefault<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Max(this IDocumentQuery<int> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Max(this IRawDocumentQuery<int> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Max(this IDocumentQuery<Int32?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Max(this IRawDocumentQuery<Int32?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Max(this IDocumentQuery<System.Int64> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Max(this IRawDocumentQuery<System.Int64> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Max(this IDocumentQuery<Int64?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Max(this IRawDocumentQuery<Int64?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Max(this IDocumentQuery<System.Double> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Max(this IRawDocumentQuery<System.Double> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Max(this IDocumentQuery<Double?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Max(this IRawDocumentQuery<Double?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Max(this IDocumentQuery<System.Single> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Max(this IRawDocumentQuery<System.Single> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Max(this IDocumentQuery<Single?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Max(this IRawDocumentQuery<Single?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Max(this IDocumentQuery<decimal> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Max(this IRawDocumentQuery<decimal> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Max(this IDocumentQuery<Decimal?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Max(this IRawDocumentQuery<Decimal?> source)
        {
            return Enumerable.Max(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Max<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Max<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Max<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Max<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Max<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Max<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Max<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TResult Max<TSource, TResult>(this IDocumentQuery<TSource> source, Func<TSource, TResult> selector)
        {
            return Enumerable.Max<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TResult Max<TSource, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, TResult> selector)
        {
            return Enumerable.Max<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Min(this IDocumentQuery<int> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Min(this IRawDocumentQuery<int> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Min(this IDocumentQuery<Int32?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Min(this IRawDocumentQuery<Int32?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Min(this IDocumentQuery<System.Int64> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Min(this IRawDocumentQuery<System.Int64> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Min(this IDocumentQuery<Int64?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Min(this IRawDocumentQuery<Int64?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Min(this IDocumentQuery<System.Single> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Min(this IRawDocumentQuery<System.Single> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Min(this IDocumentQuery<Single?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Min(this IRawDocumentQuery<Single?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Min(this IDocumentQuery<System.Double> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Min(this IRawDocumentQuery<System.Double> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Min(this IDocumentQuery<Double?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Min(this IRawDocumentQuery<Double?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Min(this IDocumentQuery<decimal> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Min(this IRawDocumentQuery<decimal> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Min(this IDocumentQuery<Decimal?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Min(this IRawDocumentQuery<Decimal?> source)
        {
            return Enumerable.Min(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Min<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Min<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Min<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Min<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int32? Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Int64? Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Min<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Min<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Min<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TResult Min<TSource, TResult>(this IDocumentQuery<TSource> source, Func<TSource, TResult> selector)
        {
            return Enumerable.Min<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TResult Min<TSource, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, TResult> selector)
        {
            return Enumerable.Min<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderBy<TSource, TKey>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector)
        {
            return Enumerable.OrderBy<TSource, TKey>(source, keySelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderBy<TSource, TKey>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector)
        {
            return Enumerable.OrderBy<TSource, TKey>(source, keySelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderBy<TSource, TKey>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return Enumerable.OrderBy<TSource, TKey>(source, keySelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderBy<TSource, TKey>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return Enumerable.OrderBy<TSource, TKey>(source, keySelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderByDescending<TSource, TKey>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector)
        {
            return Enumerable.OrderByDescending<TSource, TKey>(source, keySelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderByDescending<TSource, TKey>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector)
        {
            return Enumerable.OrderByDescending<TSource, TKey>(source, keySelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderByDescending<TSource, TKey>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return Enumerable.OrderByDescending<TSource, TKey>(source, keySelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IOrderedEnumerable<TSource> OrderByDescending<TSource, TKey>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return Enumerable.OrderByDescending<TSource, TKey>(source, keySelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Reverse<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Reverse<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Reverse<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Reverse<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Select<TSource, TResult>(this IDocumentQuery<TSource> source, Func<TSource, TResult> selector)
        {
            return Enumerable.Select<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Select<TSource, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, TResult> selector)
        {
            return Enumerable.Select<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Select<TSource, TResult>(this IDocumentQuery<TSource> source, Func<TSource, int, TResult> selector)
        {
            return Enumerable.Select<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> Select<TSource, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, int, TResult> selector)
        {
            return Enumerable.Select<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TResult>(this IDocumentQuery<TSource> source, Func<TSource, IEnumerable<TResult>> selector)
        {
            return Enumerable.SelectMany<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, IEnumerable<TResult>> selector)
        {
            return Enumerable.SelectMany<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TResult>(this IDocumentQuery<TSource> source, Func<TSource, int, IEnumerable<TResult>> selector)
        {
            return Enumerable.SelectMany<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> SelectMany<TSource, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, int, IEnumerable<TResult>> selector)
        {
            return Enumerable.SelectMany<TSource, TResult>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Aggregate<TSource>(this IDocumentQuery<TSource> source, Func<TSource, TSource, TSource> func)
        {
            return Enumerable.Aggregate<TSource>(source, func);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource Aggregate<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, TSource, TSource> func)
        {
            return Enumerable.Aggregate<TSource>(source, func);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TAccumulate Aggregate<TSource, TAccumulate>(this IDocumentQuery<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> func)
        {
            return Enumerable.Aggregate<TSource, TAccumulate>(source, seed, func);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TAccumulate Aggregate<TSource, TAccumulate>(this IRawDocumentQuery<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> func)
        {
            return Enumerable.Aggregate<TSource, TAccumulate>(source, seed, func);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TResult Aggregate<TSource, TAccumulate, TResult>(this IDocumentQuery<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> func, Func<TAccumulate, TResult> resultSelector)
        {
            return Enumerable.Aggregate<TSource, TAccumulate, TResult>(source, seed, func, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TResult Aggregate<TSource, TAccumulate, TResult>(this IRawDocumentQuery<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> func, Func<TAccumulate, TResult> resultSelector)
        {
            return Enumerable.Aggregate<TSource, TAccumulate, TResult>(source, seed, func, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Any<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Any<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Any<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Any<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Any<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Any<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Any<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Any<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean All<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.All<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean All<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.All<TSource>(source, predicate);
        }

#if CURRENT
        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Append<TSource>(this IDocumentQuery<TSource> source, TSource element)
        {
            return Enumerable.Append<TSource>(source, element);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Append<TSource>(this IRawDocumentQuery<TSource> source, TSource element)
        {
            return Enumerable.Append<TSource>(source, element);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Prepend<TSource>(this IDocumentQuery<TSource> source, TSource element)
        {
            return Enumerable.Prepend<TSource>(source, element);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Prepend<TSource>(this IRawDocumentQuery<TSource> source, TSource element)
        {
            return Enumerable.Prepend<TSource>(source, element);
        }
#endif

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average(this IDocumentQuery<int> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average(this IRawDocumentQuery<int> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average(this IDocumentQuery<Int32?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average(this IRawDocumentQuery<Int32?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average(this IDocumentQuery<System.Int64> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average(this IRawDocumentQuery<System.Int64> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average(this IDocumentQuery<Int64?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average(this IRawDocumentQuery<Int64?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Average(this IDocumentQuery<System.Single> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Average(this IRawDocumentQuery<System.Single> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Average(this IDocumentQuery<Single?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Average(this IRawDocumentQuery<Single?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average(this IDocumentQuery<System.Double> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average(this IRawDocumentQuery<System.Double> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average(this IDocumentQuery<Double?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average(this IRawDocumentQuery<Double?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Average(this IDocumentQuery<decimal> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Average(this IRawDocumentQuery<decimal> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Average(this IDocumentQuery<Decimal?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Average(this IRawDocumentQuery<Decimal?> source)
        {
            return Enumerable.Average(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, int> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int32?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Int64> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Int64?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Single Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Single> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Single? Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Single?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Double Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Double> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Double? Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Double?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static decimal Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, decimal> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Average<TSource>(this IDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static Decimal? Average<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, Decimal?> selector)
        {
            return Enumerable.Average<TSource>(source, selector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Concat<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Concat<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Concat<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Concat<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Contains<TSource>(this IDocumentQuery<TSource> source, TSource value)
        {
            return Enumerable.Contains<TSource>(source, value);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Contains<TSource>(this IRawDocumentQuery<TSource> source, TSource value)
        {
            return Enumerable.Contains<TSource>(source, value);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Contains<TSource>(this IDocumentQuery<TSource> source, TSource value, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Contains<TSource>(source, value, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Boolean Contains<TSource>(this IRawDocumentQuery<TSource> source, TSource value, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Contains<TSource>(source, value, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Count<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Count<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Count<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Count<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Count<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Count<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static int Count<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.Count<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 LongCount<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.LongCount<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 LongCount<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.LongCount<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 LongCount<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.LongCount<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static System.Int64 LongCount<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.LongCount<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> DefaultIfEmpty<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.DefaultIfEmpty<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> DefaultIfEmpty<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.DefaultIfEmpty<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> DefaultIfEmpty<TSource>(this IDocumentQuery<TSource> source, TSource defaultValue)
        {
            return Enumerable.DefaultIfEmpty<TSource>(source, defaultValue);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> DefaultIfEmpty<TSource>(this IRawDocumentQuery<TSource> source, TSource defaultValue)
        {
            return Enumerable.DefaultIfEmpty<TSource>(source, defaultValue);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Distinct<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.Distinct<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Distinct<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.Distinct<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Distinct<TSource>(this IDocumentQuery<TSource> source, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Distinct<TSource>(source, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Distinct<TSource>(this IRawDocumentQuery<TSource> source, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Distinct<TSource>(source, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource ElementAt<TSource>(this IDocumentQuery<TSource> source, int index)
        {
            return Enumerable.ElementAt<TSource>(source, index);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource ElementAt<TSource>(this IRawDocumentQuery<TSource> source, int index)
        {
            return Enumerable.ElementAt<TSource>(source, index);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource ElementAtOrDefault<TSource>(this IDocumentQuery<TSource> source, int index)
        {
            return Enumerable.ElementAtOrDefault<TSource>(source, index);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource ElementAtOrDefault<TSource>(this IRawDocumentQuery<TSource> source, int index)
        {
            return Enumerable.ElementAtOrDefault<TSource>(source, index);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> AsEnumerable<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.AsEnumerable<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> AsEnumerable<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.AsEnumerable<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Except<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Except<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Except<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Except<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Except<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Except<TSource>(first, second, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Except<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Except<TSource>(first, second, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource First<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.First<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource First<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.First<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource First<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.First<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource First<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.First<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource FirstOrDefault<TSource>(this IDocumentQuery<TSource> source)
        {
            return Enumerable.FirstOrDefault<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource FirstOrDefault<TSource>(this IRawDocumentQuery<TSource> source)
        {
            return Enumerable.FirstOrDefault<TSource>(source);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource FirstOrDefault<TSource>(this IDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.FirstOrDefault<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static TSource FirstOrDefault<TSource>(this IRawDocumentQuery<TSource> source, Func<TSource, System.Boolean> predicate)
        {
            return Enumerable.FirstOrDefault<TSource>(source, predicate);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector)
        {
            return Enumerable.GroupBy<TSource, TKey>(source, keySelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector)
        {
            return Enumerable.GroupBy<TSource, TKey>(source, keySelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey>(source, keySelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey>(source, keySelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement>(source, keySelector, elementSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement>(source, keySelector, elementSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement>(source, keySelector, elementSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement>(source, keySelector, elementSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TResult>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TKey, IEnumerable<TSource>, TResult> resultSelector)
        {
            return Enumerable.GroupBy<TSource, TKey, TResult>(source, keySelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TKey, IEnumerable<TSource>, TResult> resultSelector)
        {
            return Enumerable.GroupBy<TSource, TKey, TResult>(source, keySelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector, Func<TKey, IEnumerable<TElement>, TResult> resultSelector)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement, TResult>(source, keySelector, elementSelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector, Func<TKey, IEnumerable<TElement>, TResult> resultSelector)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement, TResult>(source, keySelector, elementSelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TResult>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TKey, IEnumerable<TSource>, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey, TResult>(source, keySelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TKey, IEnumerable<TSource>, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey, TResult>(source, keySelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector, Func<TKey, IEnumerable<TElement>, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement, TResult>(source, keySelector, elementSelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IRawDocumentQuery<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector, Func<TKey, IEnumerable<TElement>, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupBy<TSource, TKey, TElement, TResult>(source, keySelector, elementSelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, IEnumerable<TInner>, TResult> resultSelector)
        {
            return Enumerable.GroupJoin<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IRawDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, IEnumerable<TInner>, TResult> resultSelector)
        {
            return Enumerable.GroupJoin<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, IEnumerable<TInner>, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupJoin<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IRawDocumentQuery<TOuter> outer, IEnumerable<TInner> inner, Func<TOuter, TKey> outerKeySelector, Func<TInner, TKey> innerKeySelector, Func<TOuter, IEnumerable<TInner>, TResult> resultSelector, IEqualityComparer<TKey> comparer)
        {
            return Enumerable.GroupJoin<TOuter, TInner, TKey, TResult>(outer, inner, outerKeySelector, innerKeySelector, resultSelector, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Intersect<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Intersect<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Intersect<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Intersect<TSource>(first, second);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Intersect<TSource>(this IDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Intersect<TSource>(first, second, comparer);
        }

        [Obsolete("This method is one of the 'System.Linq.Enumerable' extensions and the query will be materialized before execution of this method. It will be applied to in-memory results. If you want to get rid of this message please use '.ToList()' before execution of this method.")]
        public static IEnumerable<TSource> Intersect<TSource>(this IRawDocumentQuery<TSource> first, IEnumerable<TSource> second, IEqualityComparer<TSource> comparer)
        {
            return Enumerable.Intersect<TSource>(first, second, comparer);
        }

    }
}
