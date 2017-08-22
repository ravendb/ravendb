//-----------------------------------------------------------------------
// <copyright file="IndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// All the information required to query an index
    /// </summary>
    public class IndexQuery : IndexQuery<Parameters>
    {
        public override bool Equals(IndexQuery<Parameters> other)
        {
            return base.Equals(other);
        }

        public ulong GetQueryHash(JsonOperationContext ctx)
        {
            using (var hasher = new QueryHashCalculator(ctx))
            {
                hasher.Write(Query);
                hasher.Write(WaitForNonStaleResults);
                hasher.Write(WaitForNonStaleResultsAsOfNow);
                hasher.Write(WaitForNonStaleResultsAsOfNow);
                hasher.Write(DisableCaching);
                hasher.Write(SkipDuplicateChecking);
                hasher.Write(ShowTimings);
                hasher.Write(ExplainScores);
                hasher.Write(WaitForNonStaleResultsTimeout?.Ticks);
                hasher.Write(CutoffEtag);
                hasher.Write(Start);
                hasher.Write(PageSize);
                hasher.Write(HighlighterKeyName);
                hasher.Write(HighlighterPreTags);
                hasher.Write(HighlighterPostTags);
                hasher.Write(HighlightedFields);
                hasher.Write(QueryParameters);
                
                return hasher.GetHash();
            }
        }
    }

    public abstract class IndexQuery<T> : IndexQueryBase<T>, IEquatable<IndexQuery<T>>
    {
        /// <summary>
        /// Array of fields containing highlighting information.
        /// </summary>
        public HighlightedField[] HighlightedFields { get; set; }

        /// <summary>
        /// Array of highlighter pre tags that will be applied to highlighting results.
        /// </summary>
        public string[] HighlighterPreTags { get; set; }

        /// <summary>
        /// Array of highlighter post tags that will be applied to highlighting results.
        /// </summary>
        public string[] HighlighterPostTags { get; set; }

        /// <summary>
        /// Highlighter key name.
        /// </summary>
        public string HighlighterKeyName { get; set; }

        /// <summary>
        /// Whether we should disable caching of query results
        /// </summary>
        public bool DisableCaching { get; set; }

        /// <summary>
        /// Allow to skip duplicate checking during queries
        /// </summary>
        public bool SkipDuplicateChecking { get; set; }

        /// <summary>
        /// Whatever a query result should contain an explanation about how docs scored against query
        /// </summary>
        public bool ExplainScores { get; set; }

        /// <summary>
        /// Indicates if detailed timings should be calculated for various query parts (Lucene search, loading documents, transforming results). Default: false
        /// </summary>
        public bool ShowTimings { get; set; }

        /// <summary>
        /// Indicates if it's intersect query
        /// </summary>
        public bool IsIntersect { get; set; }

        /// <summary>
        /// Gets the custom query string variables.
        /// </summary>
        /// <returns></returns>
        protected virtual string GetCustomQueryStringVariables()
        {
            //TODO: Can remove this 
            return string.Empty;
        }

        public virtual bool Equals(IndexQuery<T> other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return base.Equals(other) &&
                   EnumerableExtension.ContentEquals(HighlightedFields, other.HighlightedFields) &&
                   EnumerableExtension.ContentEquals(HighlighterPreTags, other.HighlighterPreTags) &&
                   EnumerableExtension.ContentEquals(HighlighterPostTags, other.HighlighterPostTags) &&
                   Equals(HighlighterKeyName, other.HighlighterKeyName) &&
                   ShowTimings == other.ShowTimings &&
                   DisableCaching.Equals(other.DisableCaching) &&
                   SkipDuplicateChecking == other.SkipDuplicateChecking;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == GetType() && Equals((IndexQuery)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (HighlightedFields?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (HighlighterPreTags?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (HighlighterPostTags?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (HighlighterKeyName?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (ShowTimings ? 1 : 0);
                hashCode = (hashCode * 397) ^ (SkipDuplicateChecking ? 1 : 0);
                hashCode = (hashCode * 397) ^ DisableCaching.GetHashCode();
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
        public int Start { get; set; }

        /// <summary>
        /// Maximum number of records that will be retrieved.
        /// </summary>
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
        /// Used to calculate index staleness. When set to <c>true</c> CutOff will be set to DateTime.UtcNow on server side.
        /// </summary>
        public bool WaitForNonStaleResultsAsOfNow { get; set; }

        /// <summary>
        /// CAUTION. Used by IDocumentSession ONLY. It will have NO effect if used with IDatabaseCommands or IAsyncDatabaseCommands.
        /// </summary>
        public bool WaitForNonStaleResults { get; set; }

        public TimeSpan? WaitForNonStaleResultsTimeout { get; set; }

        /// <summary>
        /// Gets or sets the cutoff etag.
        /// <para>Cutoff etag is used to check if the index has already process a document with the given</para>
        /// <para>etag. Unlike Cutoff, which uses dates and is susceptible to clock synchronization issues between</para>
        /// <para>machines, cutoff etag doesn't rely on both the server and client having a synchronized clock and </para>
        /// <para>can work without it.</para>
        /// <para>However, when used to query map/reduce indexes, it does NOT guarantee that the document that this</para>
        /// <para>etag belong to is actually considered for the results. </para>
        /// <para>What it does it guarantee that the document has been mapped, but not that the mapped values has been reduced. </para>
        /// <para>Since map/reduce queries, by their nature, tend to be far less susceptible to issues with staleness, this is </para>
        /// <para>considered to be an acceptable trade-off.</para>
        /// <para>If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and </para>
        /// <para>use the Cutoff date option, instead.</para>
        /// </summary>
        public long? CutoffEtag { get; set; }

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
                   PageSize == other.PageSize &&
                   string.Equals(Query, other.Query) &&
                   Start == other.Start &&
                   WaitForNonStaleResultsTimeout == other.WaitForNonStaleResultsTimeout &&
                   WaitForNonStaleResultsAsOfNow.Equals(other.WaitForNonStaleResultsAsOfNow) &&
                   WaitForNonStaleResults.Equals(other.WaitForNonStaleResults) &&
                   Equals(CutoffEtag, other.CutoffEtag);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == GetType() && Equals((IndexQuery)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = PageSizeSet.GetHashCode();
                hashCode = (hashCode * 397) ^ PageSize.GetHashCode();
                hashCode = (hashCode * 397) ^ (Query?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ Start;
                hashCode = (hashCode * 397) ^ (WaitForNonStaleResultsTimeout != null ? WaitForNonStaleResultsTimeout.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ WaitForNonStaleResultsAsOfNow.GetHashCode();
                hashCode = (hashCode * 397) ^ (CutoffEtag != null ? CutoffEtag.GetHashCode() : 0);
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
    }
}
