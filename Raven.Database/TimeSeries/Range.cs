using System;

namespace Raven.Database.TimeSeries
{
	public class Range
	{
#if DEBUG
		public string DebugKey { get; set; }
#endif

		public DateTimeOffset StartAt { get; set; }
		
		public PeriodDuration Duration { get; set; }

		public RangeValue[] Values { get; set; }

		public RangeValue Value
		{
			get { return Values[0]; }
		}

		public class RangeValue
		{
			// Position 1
			public double Volume { get; set; }

			// Position 2
			public double High { get; set; }

			// Position 3
			public double Low { get; set; }

			// Position 4
			public double Open { get; set; }

			// Position 5
			public double Close { get; set; }

			// Position 6
			public double Sum { get; set; }

			internal const int StorageItemsLength = 6;
		}
	}
}