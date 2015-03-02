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
}