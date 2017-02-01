using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    public class AppendEntriesResponse
    {
        public long LastLogIndex { get; set; }

        public bool Success { get; set; } 

        public string Message { get; set; }

        //This is used when a follower and a leader need to agree on a matched index
        public Negotiation Negotiation { get; set; }
    }

    public class Negotiation
    {
        public long MidpointIndex { get; set; }
        public long MidpointTerm  { get; set; }
        public long MinIndex { get; set; }
        public long MaxIndex { get; set; }
    }
}
