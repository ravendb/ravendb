using System;

namespace Rachis.Messages
{
    public class LogEntry
    {
        public long Index { get; set; }
        public long Term { get; set; }
        public bool? IsTopologyChange { get; set; }
        public byte[] Data { get; set; }

        public override string ToString()
        {
            return $"Index={Index}, Term={Term} IsTopologyChange={IsTopologyChange ?? false}{Environment.NewLine}Data={Convert.ToBase64String(Data)}";
        }
    }
}
