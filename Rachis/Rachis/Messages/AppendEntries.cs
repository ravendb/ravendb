using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Storage;

namespace Rachis.Messages
{
    public class AppendEntries : InitialMessageBase
    {
        public long Term { get; set; }
        public long PrevLogIndex { get; set; }
        public long PrevLogTerm { get; set; }
        public LogEntry[] Entries { get; set; }
        public int EntriesCount { get { return Entries == null ? 0 : Entries.Length; } }
        public long LeaderCommit { get; set; }
        public override MessageType GetMessageType()
        {
            return MessageType.AppendEntries;
        }
    }
}
