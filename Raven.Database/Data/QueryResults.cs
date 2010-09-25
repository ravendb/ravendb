using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Data
{
    public class QueryResults
    {
        public int LastScannedResult { get; set; }
        public JObject[] Results { get; set; }
        public string[] Errors { get; set; }
        public int TotalResults { get; set; }
    }
}