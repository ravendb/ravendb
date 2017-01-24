using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    public class AppendEntriesResponse : MessageBase
    {
        public long CurrentTerm { get; set; }

        public long LastLogIndex { get; set; }

        public bool Success { get; set; }

        public string Message { get; set; }

        public string LeaderId { get; set; }

        public override MessageType GetMessageType()
        {
            return MessageType.AppendEntriesResponse;
        }
    }
}
