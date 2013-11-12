//-----------------------------------------------------------------------
// <copyright file="IndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// All the information required to query a Raven index
	/// </summary>
	public class IndexQuery : IEquatable<IndexQuery>
	{
	    private int pageSize;

		/// <summary>
		/// Initializes a new instance of the <see cref="IndexQuery"/> class.
		/// </summary>
		public IndexQuery()
		{
			TotalSize = new Reference<int>();
			SkippedResults = new Reference<int>();
			pageSize = 128;
		}

		/// <summary>
		/// Whatever the page size was explicitly set or still at its default value
		/// </summary>
		public bool PageSizeSet { get; private set; }

		/// <summary>
		/// Whatever we should apply distinct operation to the query on the server side
		/// </summary>
		public bool IsDistinct { get; set; }

		/// <summary>
		/// Gets or sets the query.
		/// </summary>
		/// <value>The query.</value>
		public string Query { get; set; }

		/// <summary>
		/// Gets or sets the total size.
		/// </summary>
		/// <value>The total size.</value>
		public Reference<int> TotalSize { get; private set; }

		public Dictionary<string, SortOptions> SortHints { get; set; } 

        /// <summary>
        /// Additional query inputs
        /// </summary>
        public Dictionary<string, RavenJToken> QueryInputs { get; set; }

		/// <summary>
		/// Gets or sets the start of records to read.
		/// </summary>
		/// <value>The start.</value>
		public int Start { get; set; }

		/// <summary>
		/// Gets or sets the size of the page.
		/// </summary>
		/// <value>The size of the page.</value>
		public int PageSize
		{
			get { return pageSize; }
			set
			{
				pageSize = value;
				PageSizeSet = true;
			}
		}

		/// <summary>
		/// Gets or sets the fields to fetch.
		/// </summary>
		/// <value>The fields to fetch.</value>
		public string[] FieldsToFetch { get; set; }

		/// <summary>
		/// Gets or sets the fields to sort by
		/// </summary>
		/// <value>The sorted fields.</value>
		public SortedField[] SortedFields { get; set; }

		/// <summary>
		/// Gets or sets the cutoff date
		/// </summary>
		/// <value>The cutoff.</value>
		public DateTime? Cutoff { get; set; }

		/// <summary>
		/// Gets or sets the cutoff etag
		/// </summary>
		/// <remarks>
		/// Cutoff etag is used to check if the index has already process a document with the given
		/// etag. Unlike Cutoff, which uses dates and is susceptible to clock synchronization issues between
		/// machines, cutoff etag doesn't rely on both the server and client having a synchronized clock and 
		/// can work without it.
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this
		/// etag belong to is actually considered for the results. 
		/// What it does it guarantee that the document has been mapped, but not that the mapped values has been reduce. 
		/// Since map/reduce queries, by their nature,tend to be far less susceptible to issues with staleness, this is 
		/// considered to be an acceptable tradeoff.
		/// If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and 
		/// use the Cutoff date option, instead.
		/// </remarks>
		public Etag CutoffEtag { get; set; }

		/// <summary>
		/// The default field to use when querying directly on the Lucene query
		/// </summary>
		public string DefaultField { get; set; }

		/// <summary>
		/// Changes the default operator mode we use for queries.
		/// When set to Or a query such as 'Name:John Age:18' will be interpreted as:
		///  Name:John OR Age:18
		/// When set to And the query will be interpreted as:
		///	 Name:John AND Age:18
		/// </summary>
		public QueryOperator DefaultOperator { get; set; }

		/// <summary>
		/// If set to true, RavenDB won't execute the transform results function
		/// returning just the raw results instead
		/// </summary>
		public bool SkipTransformResults { get; set; }

		/// <summary>
		/// Gets or sets the number of skipped results.
		/// </summary>
		/// <value>The skipped results.</value>
		public Reference<int> SkippedResults { get; set; }

		/// <summary>
		/// Whatever we should get the raw index queries
		/// </summary>
		public bool DebugOptionGetIndexEntries { get; set; }

        /// <summary>
        /// Gets or sets the options to highlight the fields
        /// </summary>
        public HighlightedField[] HighlightedFields { get; set; }

        /// <summary>
        /// Gets or sets the highlighter pre tags
        /// </summary>
	    public string[] HighlighterPreTags { get; set; }

        /// <summary>
        /// Gets or sets the highlighter post tags
        /// </summary>
	    public string[] HighlighterPostTags { get; set; }

        /// <summary>
        /// Gets or sets the results transformer
        /// </summary>
	    public string ResultsTransformer { get; set; }

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
		/// Gets the index query URL.
		/// </summary>
		public string GetIndexQueryUrl(string operationUrl, string index, string operationName, bool includePageSizeEvenIfNotExplicitlySet = true)
		{
			if (operationUrl.EndsWith("/"))
				operationUrl = operationUrl.Substring(0, operationUrl.Length - 1);
			var path = new StringBuilder()
				.Append(operationUrl)
				.Append("/")
				.Append(operationName)
				.Append("/")
				.Append(index);

			AppendQueryString(path, includePageSizeEvenIfNotExplicitlySet);

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

		public void AppendQueryString(StringBuilder path, bool includePageSizeEvenIfNotExplicitlySet = true)
		{
			path.Append("?");

			AppendMinimalQueryString(path);

			if (Start != 0)
				path.Append("&start=").Append(Start);

			if (includePageSizeEvenIfNotExplicitlySet || PageSizeSet)
				path.Append("&pageSize=").Append(PageSize);
			

			if(IsDistinct)
				path.Append("&distinct=true");

			FieldsToFetch.ApplyIfNotNull(field => path.Append("&fetch=").Append(Uri.EscapeDataString(field)));
			SortedFields.ApplyIfNotNull(
				field => path.Append("&sort=").Append(field.Descending ? "-" : "").Append(Uri.EscapeDataString(field.Field)));

			
			
            if (SkipTransformResults)
            {
                path.Append("&skipTransformResults=true");
            }

            if (string.IsNullOrEmpty(ResultsTransformer) == false)
            {
                path.AppendFormat("&resultsTransformer={0}", Uri.EscapeDataString(ResultsTransformer));
            }

			if (QueryInputs != null)
			{
				foreach (var input in QueryInputs)
				{
					path.AppendFormat("&qp-{0}={1}", input.Key, input.Value);
				}
			}

			if (Cutoff != null)
			{
				var cutOffAsString = Uri.EscapeDataString(Cutoff.Value.ToString("o", CultureInfo.InvariantCulture));
				path.Append("&cutOff=").Append(cutOffAsString);
			}
			if (CutoffEtag != null)
			{
				path.Append("&cutOffEtag=").Append(CutoffEtag);
			}

		    HighlightedFields.ApplyIfNotNull(field => path.Append("&highlight=").Append(field));
            HighlighterPreTags.ApplyIfNotNull(tag=>path.Append("&preTags=").Append(tag));
            HighlighterPostTags.ApplyIfNotNull(tag=>path.Append("&postTags=").Append(tag));

			if(DebugOptionGetIndexEntries)
				path.Append("&debug=entries");

			if (ExplainScores)
				path.Append("&explainScores=true");
		}

		private void AppendMinimalQueryString(StringBuilder path)
		{
			if (string.IsNullOrEmpty(Query) == false)
			{
				path.Append("&query=")
				    .Append(Uri.EscapeDataString(Query));
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

		/// <summary>
		/// Gets the custom query string variables.
		/// </summary>
		/// <returns></returns>
		protected virtual string GetCustomQueryStringVariables()
		{
			return string.Empty;
		}

		public IndexQuery Clone()
		{
			return (IndexQuery)MemberwiseClone();
		}

		public override string ToString()
		{
			return Query;
		}

        public bool Equals(IndexQuery other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return PageSizeSet.Equals(other.PageSizeSet) && 
                   String.Equals(Query, other.Query) && 
                   Equals(TotalSize, other.TotalSize) && 
                   Equals(QueryInputs, other.QueryInputs) && 
                   Start == other.Start && 
                   Equals(IsDistinct, other.IsDistinct) && 
                   Equals(FieldsToFetch, other.FieldsToFetch) && 
                   Equals(SortedFields, other.SortedFields) && 
                   Cutoff.Equals(other.Cutoff) && 
                   Equals(CutoffEtag, other.CutoffEtag) && 
                   String.Equals(DefaultField, other.DefaultField) && 
                   DefaultOperator == other.DefaultOperator && 
                   SkipTransformResults.Equals(other.SkipTransformResults) && 
                   Equals(SkippedResults, other.SkippedResults) && 
                   DebugOptionGetIndexEntries.Equals(other.DebugOptionGetIndexEntries) && 
                   Equals(HighlightedFields, other.HighlightedFields) && 
                   Equals(HighlighterPreTags, other.HighlighterPreTags) && 
                   Equals(HighlighterPostTags, other.HighlighterPostTags) && 
                   String.Equals(ResultsTransformer, other.ResultsTransformer) && 
                   DisableCaching.Equals(other.DisableCaching);
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
                hashCode = (hashCode * 397) ^ (Query != null ? Query.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (TotalSize != null ? TotalSize.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (QueryInputs != null ? QueryInputs.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Start;
                hashCode = (hashCode * 397) ^ (IsDistinct ? 1 : 0);
                hashCode = (hashCode * 397) ^ (FieldsToFetch != null ? FieldsToFetch.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (SortedFields != null ? SortedFields.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Cutoff.GetHashCode();
                hashCode = (hashCode * 397) ^ (CutoffEtag != null ? CutoffEtag.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (DefaultField != null ? DefaultField.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int)DefaultOperator;
                hashCode = (hashCode * 397) ^ SkipTransformResults.GetHashCode();
                hashCode = (hashCode * 397) ^ (SkippedResults != null ? SkippedResults.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ DebugOptionGetIndexEntries.GetHashCode();
                hashCode = (hashCode * 397) ^ (HighlightedFields != null ? HighlightedFields.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (HighlighterPreTags != null ? HighlighterPreTags.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (HighlighterPostTags != null ? HighlighterPostTags.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ResultsTransformer != null ? ResultsTransformer.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ DisableCaching.GetHashCode();
                return hashCode;
            }
        }

        public static bool operator ==(IndexQuery left, IndexQuery right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(IndexQuery left, IndexQuery right)
        {
            return !Equals(left, right);
        }

	}

    public enum QueryOperator
	{
		Or,
		And
	}
}
