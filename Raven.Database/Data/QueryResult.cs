using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
	/// <summary>
	/// The result of a query
	/// </summary>
	public class QueryResult
	{
		/// <summary>
		/// Gets or sets the document resulting from this query.
		/// </summary>
		/// <value>The results.</value>
		public List<JObject> Results { get; set; }
		/// <summary>
		/// Gets or sets the document included in the result.
		/// </summary>
		/// <value>The includes.</value>
		public List<JObject> Includes { get; set; }
		/// <summary>
		/// Gets or sets a value indicating whether the index is stale.
		/// </summary>
		/// <value><c>true</c> if the index is stale; otherwise, <c>false</c>.</value>
		public bool IsStale { get; set; }
		/// <summary>
		/// Gets or sets the total results for this query
		/// </summary>
		/// <value>The total results.</value>
		public int TotalResults { get; set; }
		/// <summary>
		/// Gets or sets the skipped results (duplicate documents);
		/// </summary>
		/// <value>The skipped results.</value>
        public int SkippedResults { get; set; }

		/// <summary>
		/// Initializes a new instance of the <see cref="QueryResult"/> class.
		/// </summary>
		public QueryResult()
		{
			Results = new List<JObject>();
			Includes = new List<JObject>();
		}
	}
}