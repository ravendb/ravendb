using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    [StructLayout(LayoutKind.Sequential)]
    public struct MessageHeader
    {
        public int Length;
        public MessageType Type;
    }

    public enum MessageType 
    {
        AppendEntries,
        AppendEntriesResponse,
        RequestVote,
        RequestVoteResponse,
        TermNegotiationRequest,
        LeanAppendEntries,

    }
}
