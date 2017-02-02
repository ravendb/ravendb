using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Commands;

namespace Rachis.Storage
{
    public class LogEntry
    {
        public long Index { get; set; }
        public long Term { get; set; }
        public bool? IsTopologyChange { get; set; }
        public byte[] Data { get; set; }
        //TODO:Replace the command with a serialized version. 
        //public Command Command { get; set; }
    }
}
