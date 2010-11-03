using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;
using Raven.Database.Indexing;

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
            var path = string.Format("{0}/{5}/{1}?query={2}&start={3}&pageSize={4}", operationUrl, index,
                                     Uri.EscapeUriString(Uri.EscapeDataString(Query ?? "")),
                                     Start, PageSize, operationName);
            if (FieldsToFetch != null && FieldsToFetch.Length > 0)
            {
                path = FieldsToFetch.Aggregate(
                    new StringBuilder(path),
                    (sb, field) => sb.Append("&fetch=").Append(Uri.EscapeDataString(field))
                    ).ToString();
            }
            if (SortedFields != null && SortedFields.Length > 0)
            {
                path = SortedFields.Aggregate(
                    new StringBuilder(path),
                    (sb, field) => sb.Append("&sort=").Append(field.Descending ? "-" : "").Append(Uri.EscapeDataString(field.Field))
                    ).ToString();
            }
            if (Cutoff != null)
            {
                var cutOffAsString = Uri.EscapeUriString(Uri.EscapeDataString(Cutoff.Value.ToString("o", CultureInfo.InvariantCulture)));
                path = path + "&cutOff=" + cutOffAsString;
            }
		    var vars = GetCustomQueryStringVariables();

			if (!string.IsNullOrEmpty(vars))
			{
				path += vars.StartsWith("&") ? vars : ("&" + vars);
			}
		
			return path;
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
}
