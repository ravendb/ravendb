namespace Raven.Abstractions.Counters
{
	public class CounterSummary
	{
		public string Group { get; set; }

		public string CounterName { get; set; }

		public long Total { get; set; }
	}
}