using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Storage;

namespace Rachis.Messages
{
    public class LeanAppendEntries:MessageBase
    {
        public LogEntry[] Entries { get; set; }
        public long LeaderCommit { get; set; }
        public override MessageType GetMessageType()
        {
            return MessageType.LeanAppendEntries;
        }
    }
}
