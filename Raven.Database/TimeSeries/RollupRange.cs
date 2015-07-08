using System;

namespace Raven.Database.TimeSeries
{
	public class RollupRange
	{
		public string Prefix { get; set; }
		
		public byte FullPrefixLength { get; set; }
		
		public string Key { get; set; }

		public RollupRange(string prefix, string key, DateTime time)
		{
			Prefix = prefix;
			Key = key;
			Start = End = time;
		}

		public DateTime Start { get; set; }

		public DateTime End { get; set; }
	}
}