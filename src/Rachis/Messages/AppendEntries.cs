using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Commands;
using Rachis.Storage;

namespace Rachis.Messages
{
    public class AppendEntries 
    {
        public long Term { get; set; }
        public long PrevLogIndex { get; set; }
        public long PrevLogTerm { get; set; }
        public LogEntry[] Entries { get; set; }
        public int EntriesCount => Entries?.Length ?? 0;
        public long LeaderCommit { get; set; }

        //this is the last topology change command in the entries, or null if topology didn't change.
        //public TopologyChangeCommand TopologyChange { get; set; }
        public int PositionOfTopologyChange { get; set; } = -1;
    }
}
