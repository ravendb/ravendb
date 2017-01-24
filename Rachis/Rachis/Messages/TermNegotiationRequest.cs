using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    public class TermNegotiationRequest : MessageBase
    {
        public bool Success { get; set; }
        public long MidpointIndex { get; set; }
        public long MidpointTerm { get; set; }
        public long MaxIndex { get; set; }
        public long MinIndex { get; set; }

        public override MessageType GetMessageType()
        {
            return MessageType.TermNegotiationRequest;
        }
    }
}
