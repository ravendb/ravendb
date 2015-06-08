using System;

namespace Raven.Database.Counters
{
	public class CounterSummary
	{
		public CounterSummary()
		{
			Increments = 0;
			Decrements = 0;
		}

		public string Group { get; set; }

		public string CounterName { get; set; }

		public long Increments { get; set; }

		public long Decrements { get; set; }

		public long Total
		{
			get
			{
				return Increments - Decrements;
			}
		}
	}
}