using System;

namespace Raven.Client.Documents.Session
{
    public sealed class StreamQueryStatistics
    {
        public string IndexName { get; set; }
        public bool IsStale { get; set; }
        public DateTime IndexTimestamp { get; set; }
        public int TotalResults { get; set; }
        public long ResultEtag { get; set; }
    }
}