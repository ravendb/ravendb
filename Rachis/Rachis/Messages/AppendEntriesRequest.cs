using Raven.Imports.Newtonsoft.Json;

namespace Rachis.Messages
{
    public class AppendEntriesRequest : BaseMessage
    {
        public long Term { get; set; }
        public long PrevLogIndex { get; set; }
        public long PrevLogTerm { get; set; }
        [JsonIgnore]
        public LogEntry[] Entries { get; set; }
        public int EntriesCount { get { return Entries == null ? 0 : Entries.Length; } }
        public long LeaderCommit { get; set; }
    }
    public class AppendEntriesRequestWithEntries : BaseMessage
    {
        public static AppendEntriesRequestWithEntries FromAppendEntriesRequest(AppendEntriesRequest message)
        {
            return new AppendEntriesRequestWithEntries
            {
                Term = message.Term,
                PrevLogIndex = message.PrevLogIndex,
                PrevLogTerm = message.PrevLogTerm,
                Entries = message.Entries,
                LeaderCommit = message.LeaderCommit,
                From = message.From,
                ClusterTopologyId = message.ClusterTopologyId
            };
        }
        public long Term { get; set; }
        public long PrevLogIndex { get; set; }
        public long PrevLogTerm { get; set; }
        public LogEntry[] Entries { get; set; }
        public int EntriesCount { get { return Entries == null ? 0 : Entries.Length; } }
        public long LeaderCommit { get; set; }
    }
}
