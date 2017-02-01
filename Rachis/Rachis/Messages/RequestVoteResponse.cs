using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    public class RequestVoteResponse 
    {
        public MessageType GetMessageType()
        {
            return MessageType.RequestVoteResponse;
        }
    }
}
