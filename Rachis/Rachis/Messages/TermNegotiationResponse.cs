using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    public class TermNegotiationResponse : MessageBase
    {
        public bool Done { get; set; }
        public long MidpointIndex { get; set; }
        public long MidpointTerm { get; set; }
        public override MessageType GetMessageType()
        {
            return MessageType.TermNegotiationResponse;
        }
    }
}
