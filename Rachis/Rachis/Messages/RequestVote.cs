using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    public class RequestVote : InitialMessageBase
    {
        public override MessageType GetMessageType()
        {
            return MessageType.RequestVote;
        }
    }
}
