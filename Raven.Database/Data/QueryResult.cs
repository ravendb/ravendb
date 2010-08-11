using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
	public class QueryResult
	{
		public List<JObject> Results { get; set; }
		public List<JObject> Includes { get; set; }
		public bool IsStale { get; set; }
		public int TotalResults { get; set; }
        public int SkippedResults { get; set; }

		public QueryResult()
		{
			Results = new List<JObject>();
			Includes = new List<JObject>();
		}
	}
}