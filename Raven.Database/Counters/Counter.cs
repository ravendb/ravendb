using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Database.Counters
{
	public class Counter
	{
		public Counter()
		{
			CounterValues = new List<CounterValue>();
		}

		public long CalculateTotal()
		{
			return CounterValues.Sum(x => x.IsPositive ? x.Value : -x.Value);
		}

		public List<CounterValue> CounterValues { get; private set; }

		public class PerServerValue
		{
			public string ServerName { get; set; }
			public Guid ServerId { get; set; }
			public long Positive { get; set; }
			public long Negative { get; set; }
		}
	}
}