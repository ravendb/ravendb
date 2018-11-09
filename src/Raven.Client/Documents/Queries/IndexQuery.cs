//-----------------------------------------------------------------------
// <copyright file="IndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// All the information required to query an index
    /// </summary>
    public class IndexQuery : IndexQuery<Parameters>
    {
        /// <summary>
        /// Indicates if query results should be read from cache (if cached previously) or added to cache (if there were no cached items prior)
        /// </summary>
        public bool DisableCaching { get; set; }

        public ulong GetQueryHash(JsonOperationContext ctx)
        {
            using (var hasher = new HashCalculator(ctx))
            {
                hasher.Write(Query);
                hasher.Write(WaitForNonStaleResults);
                hasher.Write(SkipDuplicateChecking);
                hasher.Write(WaitForNonStaleResultsTimeout?.Ticks);
#pragma warning disable 618
                hasher.Write(Start);
                hasher.Write(PageSize);
#pragma warning restore 618
                hasher.Write(QueryParameters);

                return hasher.GetHash();
            }
        }

        public override bool Equals(IndexQuery<Parameters> other)
        {
            if (base.Equals(other) == false)
                return false;

            if (other is IndexQuery iq && DisableCaching.Equals(iq.DisableCaching))
                return true;

            return false;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ DisableCaching.GetHashCode();
                return hashCode;
            }
        }
    }

    public abstract class IndexQuery<T> : IndexQueryBase<T>, IEquatable<IndexQuery<T>>
    {
        /// <summary>
        /// Allow to skip duplicate checking during queries
        /// </summary>
        public bool SkipDuplicateChecking { get; set; }

        public virtual bool Equals(IndexQuery<T> other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return base.Equals(other) &&
                   SkipDuplicateChecking == other.SkipDuplicateChecking;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            return obj.GetType() == GetType() && Equals((IndexQuery)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (SkipDuplicateChecking ? 1 : 0);
                return hashCode;
            }
        }

        public static bool operator ==(IndexQuery<T> left, IndexQuery<T> right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(IndexQuery<T> left, IndexQuery<T> right)
        {
            return Equals(left, right) == false;
        }
    }

    public abstract class IndexQueryBase<T> : IIndexQuery, IEquatable<IndexQueryBase<T>>
    {
        private int _pageSize = int.MaxValue;

        /// <summary>
        /// Whether the page size was explicitly set or still at its default value
        /// </summary>
        protected internal bool PageSizeSet { get; private set; }

        /// <summary>
        /// Actual query that will be performed (Lucene syntax).
        /// </summary>
        public string Query { get; set; }

        public T QueryParameters { get; set; }

        /// <summary>
        /// Number of records that should be skipped.
        /// </summary>
        [Obsolete("Use OFFSET in RQL instead")]
        public int Start { get; set; }

        /// <summary>
        /// Maximum number of records that will be retrieved.
        /// </summary>
        [Obsolete("Use LIMIT in RQL instead")]
        public int PageSize
        {
            get => _pageSize;
            set
            {
                _pageSize = value;
                PageSizeSet = true;
            }
        }

        /// <summary>
        /// When set to <c>true</c>> server side will wait until result are non stale or until timeout
        /// </summary>
        public bool WaitForNonStaleResults { get; set; }

        public TimeSpan? WaitForNonStaleResultsTimeout { get; set; }

        public override string ToString()
        {
            return Query;
        }

        public virtual bool Equals(IndexQueryBase<T> other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return PageSizeSet.Equals(other.PageSizeSet) &&
#pragma warning disable 618
                   PageSize == other.PageSize &&
                   Start == other.Start &&
#pragma warning restore 618
                   string.Equals(Query, other.Query) &&
                   WaitForNonStaleResultsTimeout == other.WaitForNonStaleResultsTimeout &&
                   WaitForNonStaleResults.Equals(other.WaitForNonStaleResults);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            return obj.GetType() == GetType() && Equals((IndexQuery)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = PageSizeSet.GetHashCode();
#pragma warning disable 618
                hashCode = (hashCode * 397) ^ PageSize.GetHashCode();
                hashCode = (hashCode * 397) ^ Start;
#pragma warning restore 618
                hashCode = (hashCode * 397) ^ (Query?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (WaitForNonStaleResultsTimeout != null ? WaitForNonStaleResultsTimeout.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(IndexQueryBase<T> left, IndexQueryBase<T> right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(IndexQueryBase<T> left, IndexQueryBase<T> right)
        {
            return Equals(left, right) == false;
        }
    }

    public interface IIndexQuery
    {
        int PageSize { set; get; }

        TimeSpan? WaitForNonStaleResultsTimeout { get; }
    }
}
