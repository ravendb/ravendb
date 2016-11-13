//-----------------------------------------------------------------------
// <copyright file="IndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Data
{
    /// <summary>
    /// All the information required to query an index
    /// </summary>
    public class IndexQuery : IndexQuery<Dictionary<string, RavenJToken>>
    {
        public static int DefaultPageSize = 128;

        public override bool Equals(IndexQuery<Dictionary<string, RavenJToken>> other)
        {
            return base.Equals(other) && DictionaryExtensions.ContentEquals(TransformerParameters, other.TransformerParameters);
        }

        /// <summary>
        /// Gets the index query URL.
        /// </summary>
        public string GetIndexQueryUrl(string operationUrl, string index, string operationName, bool includePageSizeEvenIfNotExplicitlySet = true, bool includeQuery = true)
        {
            if (operationUrl.EndsWith("/"))
                operationUrl = operationUrl.Substring(0, operationUrl.Length - 1);
            var path = new StringBuilder()
                .Append(operationUrl)
                .Append("/")
                .Append(operationName)
                .Append("/")
                .Append(index);

            AppendQueryString(path, includePageSizeEvenIfNotExplicitlySet, includeQuery);

            return path.ToString();
        }

        // TODO Iftah, merge the two GetIndexQueryUrl()
        /// <summary>
        /// Gets the index query URL.
        /// </summary>
        public string GetIndexQueryUrl(string index, string operationName,
            bool includePageSizeEvenIfNotExplicitlySet = true, bool includeQuery = true)
        {
            var path = new StringBuilder();

            path.Append(operationName)
                .Append("/")
                .Append(index);

            AppendQueryString(path, includePageSizeEvenIfNotExplicitlySet, includeQuery);

            return path.ToString();
        }

        public string GetMinimalQueryString()
        {
            var sb = new StringBuilder();
            AppendMinimalQueryString(sb);
            return sb.ToString();
        }


        public string GetQueryString()
        {
            var sb = new StringBuilder();
            AppendQueryString(sb);
            return sb.ToString();
        }

        public void AppendQueryString(StringBuilder path, bool includePageSizeEvenIfNotExplicitlySet = true, bool includeQuery = true)
        {
            path.Append("?");

            AppendMinimalQueryString(path, includeQuery);

            if (Start != 0)
                path.Append("&start=").Append(Start);

            if (includePageSizeEvenIfNotExplicitlySet || PageSizeSet)
                path.Append("&pageSize=").Append(PageSize);

            if (AllowMultipleIndexEntriesForSameDocumentToResultTransformer)
                path.Append("&allowMultipleIndexEntriesForSameDocumentToResultTransformer=true");

            if (IsDistinct)
                path.Append("&distinct=true");

            if (ShowTimings)
                path.Append("&showTimings=true");
            if (SkipDuplicateChecking)
                path.Append("&skipDuplicateChecking=true");

            FieldsToFetch.ApplyIfNotNull(field => path.Append("&fetch=").Append(Uri.EscapeDataString(field)));
            Includes.ApplyIfNotNull(include => path.AppendFormat("&include={0}", Uri.EscapeDataString(include)));

            DynamicMapReduceFields.ApplyIfNotNull(field => path.Append("&mapReduce=")
                        .Append(Uri.EscapeDataString(field.Name))
                        .Append("-")
                        .Append(field.OperationType)
                        .Append("-")
                        .Append(field.IsGroupBy));

            SortedFields.ApplyIfNotNull(
                field => path.Append("&sort=").Append(field.Descending ? "-" : "").Append(Uri.EscapeDataString(field.Field)));

            if (string.IsNullOrEmpty(Transformer) == false)
            {
                path.AppendFormat("&transformer={0}", Uri.EscapeDataString(Transformer));
            }

            if (TransformerParameters != null)
            {
                foreach (var input in TransformerParameters)
                {
                    path.AppendFormat("&tp-{0}={1}", input.Key, input.Value);
                }
            }

            if (CutoffEtag != null)
            {
                path.Append("&cutOffEtag=").Append(CutoffEtag);
            }

            if (WaitForNonStaleResultsAsOfNow)
            {
                path.Append("&waitForNonStaleResultsAsOfNow=true");
            }

            if (WaitForNonStaleResultsTimeout != null)
            {
                path.AppendLine("&waitForNonStaleResultsTimeout=" + WaitForNonStaleResultsTimeout);
            }

            HighlightedFields.ApplyIfNotNull(field => path.Append("&highlight=").Append(field));
            HighlighterPreTags.ApplyIfNotNull(tag => path.Append("&preTags=").Append(tag));
            HighlighterPostTags.ApplyIfNotNull(tag => path.Append("&postTags=").Append(tag));

            if (string.IsNullOrEmpty(HighlighterKeyName) == false)
            {
                path.AppendFormat("&highlighterKeyName={0}", Uri.EscapeDataString(HighlighterKeyName));
            }

            if (DebugOptionGetIndexEntries)
                path.Append("&debug=entries");

            if (ExplainScores)
                path.Append("&explainScores=true");
        }

        private void AppendMinimalQueryString(StringBuilder path, bool appendQuery = true)
        {
            if (string.IsNullOrEmpty(Query) == false && appendQuery)
            {
                path.Append("&query=");
                path.Append(EscapingHelper.EscapeLongDataString(Query));
            }

            if (string.IsNullOrEmpty(DefaultField) == false)
            {
                path.Append("&defaultField=").Append(Uri.EscapeDataString(DefaultField));
            }
            if (DefaultOperator != QueryOperator.Or)
                path.Append("&operator=AND");
            var vars = GetCustomQueryStringVariables();
            if (!string.IsNullOrEmpty(vars))
            {
                path.Append(vars.StartsWith("&") ? vars : ("&" + vars));
            }
        }
    }

    public abstract class IndexQuery<T> : IndexQueryBase, IEquatable<IndexQuery<T>>
    {
        /// <summary>
        /// Parameters that will be passed to transformer (if specified).
        /// </summary>
        public T TransformerParameters { get; set; }

        /// <summary>
        /// Array of fields containing sorting information.
        /// </summary>
        public SortedField[] SortedFields { get; set; }

        /// <summary>
        /// Array of fields in a dynamic map reduce-query
        /// </summary>
        internal DynamicMapReduceField[] DynamicMapReduceFields { get; set; }

        /// <summary>
        /// If set to <c>true</c>, this property will send multiple index entries from the same document (assuming the index project them)
        /// <para>to the result transformer function. Otherwise, those entries will be consolidate an the transformer will be </para>
        /// <para>called just once for each document in the result set</para>
        /// </summary>
        public bool AllowMultipleIndexEntriesForSameDocumentToResultTransformer { get; set; }

        /// <summary>
        /// Whatever we should get the raw index entries.
        /// </summary>
        public bool DebugOptionGetIndexEntries { get; set; }

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
        /// Name of transformer to use on query results.
        /// </summary>
        public string Transformer { get; set; }

        /// <summary>
        /// Whatever we should disable caching of query results
        /// </summary>
        public bool DisableCaching { get; set; }

        /// <summary>
        /// Allow to skip duplicate checking during queries
        /// </summary>
        public bool SkipDuplicateChecking { get; set; }

        /// <summary>
        /// Whatever a query result should contains an explanation about how docs scored against query
        /// </summary>
        public bool ExplainScores { get; set; }

        /// <summary>
        /// Indicates if detailed timings should be calculated for various query parts (Lucene search, loading documents, transforming results). Default: false
        /// </summary>
        public bool ShowTimings { get; set; }

        /// <summary>
        /// An array of relative paths that specify related documents ids which should be included in a query result
        /// </summary>
        public string[] Includes { get; set; }

        /// <summary>
        /// Gets the custom query string variables.
        /// </summary>
        /// <returns></returns>
        protected virtual string GetCustomQueryStringVariables()
        {
            return string.Empty;
        }

        public virtual bool Equals(IndexQuery<T> other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return base.Equals(other) &&
                   EnumerableExtension.ContentEquals(SortedFields, other.SortedFields) &&
                   DebugOptionGetIndexEntries.Equals(other.DebugOptionGetIndexEntries) &&
                   EnumerableExtension.ContentEquals(HighlightedFields, other.HighlightedFields) &&
                   EnumerableExtension.ContentEquals(HighlighterPreTags, other.HighlighterPreTags) &&
                   EnumerableExtension.ContentEquals(HighlighterPostTags, other.HighlighterPostTags) &&
                   Equals(HighlighterKeyName, other.HighlighterKeyName) &&
                   string.Equals(Transformer, other.Transformer) &&
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
                hashCode = (hashCode * 397) ^ (TransformerParameters != null ? TransformerParameters.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (SortedFields?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ DebugOptionGetIndexEntries.GetHashCode();
                hashCode = (hashCode * 397) ^ (HighlightedFields?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (HighlighterPreTags?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (HighlighterPostTags?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (HighlighterKeyName?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Transformer?.GetHashCode() ?? 0);
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

    public abstract class IndexQueryBase : IEquatable<IndexQueryBase>
    {
        private int _pageSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexQueryBase"/> class.
        /// </summary>
        protected IndexQueryBase()
        {
            _pageSize = IndexQuery.DefaultPageSize;
        }

        /// <summary>
        /// Whatever the page size was explicitly set or still at its default value
        /// </summary>
        protected bool PageSizeSet { get; private set; }

        /// <summary>
        /// Whatever we should apply distinct operation to the query on the server side
        /// </summary>
        public bool IsDistinct { get; set; }

        /// <summary>
        /// Actual query that will be performed (Lucene syntax).
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// Number of records that should be skipped.
        /// </summary>
        public int Start { get; set; }

        /// <summary>
        /// Maximum number of records that will be retrieved.
        /// </summary>
        public int PageSize
        {
            get
            {
                return _pageSize;
            }
            set
            {
                _pageSize = value;
                PageSizeSet = true;
            }
        }

        /// <summary>
        /// Array of fields that will be fetched.
        /// <para>Fetch order:</para>
        /// <para>1. Stored index fields</para>
        /// <para>2. Document</para>
        /// </summary>
        public string[] FieldsToFetch { get; set; }

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
        /// <para>Since map/reduce queries, by their nature,vtend to be far less susceptible to issues with staleness, this is </para>
        /// <para>considered to be an acceptable tradeoff.</para>
        /// <para>If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and </para>
        /// <para>use the Cutoff date option, instead.</para>
        /// </summary>
        public long? CutoffEtag { get; set; }

        /// <summary>
        /// Default field to use when querying directly on the Lucene query
        /// </summary>
        public string DefaultField { get; set; }

        /// <summary>
        /// Changes the default operator mode we use for queries.
        /// <para>When set to Or a query such as 'Name:John Age:18' will be interpreted as:</para>
        /// <para> Name:John OR Age:18</para>
        /// <para>When set to And the query will be interpreted as:</para>
        ///	<para> Name:John AND Age:18</para>
        /// </summary>
        public QueryOperator DefaultOperator { get; set; }

        public override string ToString()
        {
            return Query;
        }

        public virtual bool Equals(IndexQueryBase other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return PageSizeSet.Equals(other.PageSizeSet) &&
                   string.Equals(Query, other.Query) &&
                   Start == other.Start &&
                   IsDistinct == other.IsDistinct &&
                   EnumerableExtension.ContentEquals(FieldsToFetch, other.FieldsToFetch) &&
                   WaitForNonStaleResultsTimeout == other.WaitForNonStaleResultsTimeout &&
                   WaitForNonStaleResultsAsOfNow.Equals(other.WaitForNonStaleResultsAsOfNow) &&
                   WaitForNonStaleResults.Equals(other.WaitForNonStaleResults) &&
                   Equals(CutoffEtag, other.CutoffEtag) &&
                   string.Equals(DefaultField, other.DefaultField) &&
                   DefaultOperator == other.DefaultOperator;
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
                hashCode = (hashCode * 397) ^ (Query?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ Start;
                hashCode = (hashCode * 397) ^ (IsDistinct ? 1 : 0);
                hashCode = (hashCode * 397) ^ (FieldsToFetch?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (WaitForNonStaleResultsTimeout != null ? WaitForNonStaleResultsTimeout.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ WaitForNonStaleResultsAsOfNow.GetHashCode();
                hashCode = (hashCode * 397) ^ (CutoffEtag != null ? CutoffEtag.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (DefaultField?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (int)DefaultOperator;
                return hashCode;
            }
        }

        public static bool operator ==(IndexQueryBase left, IndexQueryBase right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(IndexQueryBase left, IndexQueryBase right)
        {
            return Equals(left, right) == false;
        }
    }

    public enum QueryOperator
    {
        Or,
        And
    }
}
