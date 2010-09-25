using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
    public class QueryResults
    {
        public int LastResult { get; set; }
        public JObject[] Results { get; set; }
        public string[] Errors { get; set; }
    }
}