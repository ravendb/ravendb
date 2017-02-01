using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Rachis.Messages
{
    [StructLayout(LayoutKind.Explicit)]
    public struct MessageHeader
    {
        [FieldOffset(0)]
        public int Length;
        [FieldOffset(4)]
        public MessageType Type;
        public static MessageHeader FromBytes(byte[] buffer, int start)
        {
            return new MessageHeader { Length = BitConverter.ToInt32(buffer,start), Type = (MessageType)BitConverter.ToInt32(buffer, start+4) };
        }
        public static void ToBytes(byte[] buffer, int start,int l, MessageType t)
        {
            var length = BitConverter.GetBytes(l);
            var type = BitConverter.GetBytes((int) t);
            for (var i = 0; i < length.Length; i++)
            {
                buffer[i + start] = length[i];
            }
            for (var i = 0; i < type.Length; i++)
            {
                buffer[i + start+ sizeof(int)] = type[i];
            }
        }
    }
    
    public enum MessageType 
    {
        AppendEntries,
        AppendEntriesResponse,
        RequestVote,
        RequestVoteResponse,
    }
}
