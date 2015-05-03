using System;

namespace Raven.Database.Counters
{
	public class CounterValue
	{
		public string FullName { get; set; }

		public Guid ServerId
		{
			get
			{
				// foo/bar/260D8EDB-FD64-4F0A-A78E-6DBFA8BCD0CE/+
				// guid string length is 36 characters
				return Guid.Parse(FullName.Substring(FullName.Length - 36 - 2, 36));
			}
		}

		public long Value { get; set; }

		public bool IsPositive { get { return FullName[FullName.Length - 1] == ValueSign.Positive; } }		
	}
}