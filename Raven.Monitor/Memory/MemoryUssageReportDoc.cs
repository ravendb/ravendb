using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Monitor.Memory
{
    public class MemoryUssageReportDoc
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public List<MemoryUssageSample> Sampling { get; set; }

    }

    public class MemoryUssageSample
    {
        public DateTime Time { get; set; }
        public long UsedMemoryInBytes { get; set; }
    }
}
