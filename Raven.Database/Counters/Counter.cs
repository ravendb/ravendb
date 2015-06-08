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

		public List<CounterValue> CounterValues { get; private set; }

		public long Etag { get; set; }
	}
}