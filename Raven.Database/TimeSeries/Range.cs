using System;

namespace Raven.Database.TimeSeries
{
	public class Range
	{
#if DEBUG
		public string DebugKey { get; set; }
#endif

		public DateTime StartAt { get; set; }
		
		public PeriodDuration Duration { get; set; }

		public double High { get; set; }

		public double Low { get; set; }

		public double Open { get; set; }

		public double Close { get; set; }

		public int Volume { get; set; }

		public double Sum { get; set; }

	}
}