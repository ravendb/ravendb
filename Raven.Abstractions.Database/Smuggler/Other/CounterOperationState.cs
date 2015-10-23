namespace Raven.Abstractions.Database.Smuggler.Other
{
	public class CounterOperationState
	{
		public long LastWrittenEtag { get; set; }

		public string CounterId { get; set; }
	}
}
