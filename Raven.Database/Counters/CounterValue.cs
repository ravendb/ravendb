using System;
using System.Text;
using Mono.CSharp;

namespace Raven.Database.Counters
{
	public class CounterValue
	{
		private readonly string fullName;

		public long Value { get; private set; }

		private readonly Lazy<string[]> splittedFullName;

		public CounterValue(string fullName, long value)
		{
			this.fullName = fullName;
			splittedFullName = new Lazy<string[]>(() =>
			{
				var splittedName = this.fullName.Split('/');
				if (splittedName.Length != 4)
					throw new Exception("Counter name is in wrong format!");
				return splittedName;
			});
			Value = value;
		}

		public string Group()
		{
			return splittedFullName.Value[0];
		}

		public string CounterName()
		{
			return splittedFullName.Value[1];
		}

		public Guid ServerId()
		{
			// guid string length is 36 characters
			return Guid.Parse(fullName.Substring(fullName.Length - 36 - 2, 36));
		}

		public bool IsPositive()
		{
			throw new NotImplementedException();
			//return fullName.Substring(fullName.Length - 1, 1) == ValueSign.Positive;
		}
	}
}