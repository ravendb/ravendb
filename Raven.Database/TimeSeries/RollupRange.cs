using System;

namespace Raven.Database.TimeSeries
{
	public class RollupRange
	{
		public string Type { get; set; }
		
		public string Key { get; set; }

		public RollupRange(string type, string key, DateTime time)
		{
			Type = type;
			Key = key;
			Start = End = time;
		}

		public DateTime Start { get; set; }

		public DateTime End { get; set; }
	}
}