using System;

namespace Raven.Database.TimeSeries
{
	public class RollupRange
	{
		public string Type { get; set; }
		
		public string Key { get; set; }

		public RollupRange(string type, string key, DateTimeOffset time)
		{
			Type = type;
			Key = key;
			Start = End = time;
		}

		public DateTimeOffset Start { get; set; }

		public DateTimeOffset End { get; set; }
	}
}