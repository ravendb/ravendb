using System;

namespace Raven.Database.LinearQueries
{
    [Serializable]
    public class RemoteQueryResults
    {
        public string[] Results { get; set; }
        public string[] Errors { get; set; }
        public int QueryCacheSize { get; set; }
        public int LastScannedResult { get; set; }
        public int TotalResults { get; set; }
    }
}