using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Monitor.IO.Data
{
	public class DiskQueueReportDoc
	{
		public DateTime StartTime { get; set; }
		public DateTime EndTime { get; set; }
		public List<DiskQueueSample> Sampling { get; set; }

	}

	public class DiskQueueSample
	{
		public DateTime Time { get; set; }
		public float ReadQueueSize { get; set; }
		public float WriteQueueSize { get; set; }
	}
}
