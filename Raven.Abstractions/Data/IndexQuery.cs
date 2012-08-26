//-----------------------------------------------------------------------
// <copyright file="IndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Globalization;
using System.Text;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// All the information required to query a Raven index
	/// </summary>
	public class IndexQuery
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="IndexQuery"/> class.
		/// </summary>
		public IndexQuery()
		{
			TotalSize = new Reference<int>();
			SkippedResults = new Reference<int>();
			PageSize = 128;
		}

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

		/// <summary>
		/// Gets or sets the start of records to read.
		/// </summary>
		/// <value>The start.</value>
		public int Start { get; set; }

		/// <summary>
		/// Gets or sets the size of the page.
		/// </summary>
		/// <value>The size of the page.</value>
		public int PageSize { get; set; }

		/// <summary>
		/// The aggregation operation for this query
		/// </summary>
		public AggregationOperation AggregationOperation { get; set; }

		/// <summary>
		/// The fields to group the query by
		/// </summary>
		public string[] GroupBy { get; set; }

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
		/// etag. Unlike Cutoff, which uses dates and is susceptible to clock syncronization issues between
		/// machines, cutoff etag doesn't rely on both the server and client having a syncronized clock and 
		/// can work without it.
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this
		/// etag belong to is actually considered for the results. 
		/// What it does it guarantee that the document has been mapped, but not that the mapped values has been reduce. 
		/// Since map/reduce queries, by their nature,tend to be far less susceptible to issues with staleness, this is 
		/// considered to be an acceptable tradeoff.
		/// If you need absolute no staleness with a map/reduce index, you will need to ensure syncronized clocks and 
		/// use the Cutoff date option, instead.
		/// </remarks>
		public Guid? CutoffEtag { get; set; }

		/// <summary>
		/// The default field to use when querying directly on the Lucene query
		/// </summary>
		public string DefaultField { get; set; }

		/// <summary>
		/// Changes the default operator mode we use for queries.
		/// When set to Or a query such as 'Name:John Age:18' will be interpreted as:
		///  Name:John OR Age:18
		/// When set to And the queyr will be interpreted as:
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
		/// Gets the index query URL.
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		/// <param name="index">The index.</param>
		/// <param name="operationName">Name of the operation.</param>
		/// <returns></returns>
		public string GetIndexQueryUrl(string operationUrl, string index, string operationName)
		{
			if (operationUrl.EndsWith("/"))
				operationUrl = operationUrl.Substring(0, operationUrl.Length - 1);
			var path = new StringBuilder()
				.Append(operationUrl)
				.Append("/")
				.Append(operationName)
				.Append("/")
				.Append(index);

			AppendQueryString(path);



			return path.ToString();
		}

        public string GetQueryString()
        {
            var sb = new StringBuilder();
            AppendQueryString(sb);
            return sb.ToString();
        }

		public void AppendQueryString(StringBuilder path)
		{
			path
				.Append("?query=");

			path.Append(Uri.EscapeUriString(Uri.EscapeDataString(Query ?? "")))
				.Append("&start=").Append(Start)
				.Append("&pageSize=").Append(PageSize)
				.Append("&aggregation=").Append(AggregationOperation);
			FieldsToFetch.ApplyIfNotNull(field => path.Append("&fetch=").Append(Uri.EscapeDataString(field)));
			GroupBy.ApplyIfNotNull(field => path.Append("&groupBy=").Append(Uri.EscapeDataString(field)));
			SortedFields.ApplyIfNotNull(
				field => path.Append("&sort=").Append(field.Descending ? "-" : "").Append(Uri.EscapeDataString(field.Field)));

			if(string.IsNullOrEmpty(DefaultField) == false)
			{
				path.Append("&defaultField=").Append(Uri.EscapeDataString(DefaultField));
			}

			if (DefaultOperator != QueryOperator.Or)
				path.Append("&operator=AND");
			
            if (SkipTransformResults)
            {
                path.Append("&skipTransformResults=true");
            }

			if (Cutoff != null)
			{
				var cutOffAsString =
					Uri.EscapeUriString(Uri.EscapeDataString(Cutoff.Value.ToString("o", CultureInfo.InvariantCulture)));
				path.Append("&cutOff=").Append(cutOffAsString);
			}
			if (CutoffEtag != null)
			{
				path.Append("&cutOffEtag=").Append(CutoffEtag.Value.ToString());
			}
			var vars = GetCustomQueryStringVariables();

			if (!string.IsNullOrEmpty(vars))
			{
				path.Append(vars.StartsWith("&") ? vars : ("&" + vars));
			}

			if(DebugOptionGetIndexEntries)
				path.Append("&debug=entries");
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
	}

	public enum QueryOperator
	{
		Or,
		And
	}
}
