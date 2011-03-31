//-----------------------------------------------------------------------
// <copyright file="IndexQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;

namespace Raven.Database.Data
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
				.Append(index)
				.Append("?query=").Append(Uri.EscapeUriString(Uri.EscapeDataString(Query ?? "")))
				.Append("&start=").Append(Start)
				.Append("&pageSize=").Append(PageSize)
				.Append("&aggregation=").Append(AggregationOperation);
			FieldsToFetch.ApplyIfNotNull(field => path.Append("&fetch=").Append(Uri.EscapeDataString(field)));
			GroupBy.ApplyIfNotNull(field => path.Append("&groupBy=").Append(Uri.EscapeDataString(field)));
			SortedFields.ApplyIfNotNull(field => path.Append("&sort=").Append(field.Descending ? "-" : "").Append(Uri.EscapeDataString(field.Field)));
			
			if (Cutoff != null)
			{
				var cutOffAsString =
					Uri.EscapeUriString(Uri.EscapeDataString(Cutoff.Value.ToString("o", CultureInfo.InvariantCulture)));
				path.Append("&cutOff=").Append(cutOffAsString);
			}
			var vars = GetCustomQueryStringVariables();

			if (!string.IsNullOrEmpty(vars))
			{
				path.Append(vars.StartsWith("&") ? vars : ("&" + vars));
			}

			return path.ToString();
		}

		/// <summary>
		/// Gets the custom query string variables.
		/// </summary>
		/// <returns></returns>
		protected virtual string GetCustomQueryStringVariables()
		{
			return string.Empty;
		}
	}

	internal static class EnumerableExtension
	{
		public static void ApplyIfNotNull<T>(this IEnumerable<T> self, Action<T> action)
		{
			if (self == null)
				return;
			foreach (var item in self)
			{
				action(item);
			}
		}
	}
}
