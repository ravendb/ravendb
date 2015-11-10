using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Monitor.IO.Data
{
    public class CpuUssageReportDoc
    {
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public List<CpuUssageSample> Sampling { get; set; }
    }
    public class CpuUssageSample
    {
        public DateTime Time { get; set; }
        public float UsedCpu{ get; set; }
    }
}
