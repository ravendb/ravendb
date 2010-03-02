using Newtonsoft.Json.Linq;

namespace Raven.Database
{
    public class QueryResult
    {
        public JObject[] Results { get; set; }
        public bool IsStale { get; set; }
        public int TotalResults { get; set; }
    }
}