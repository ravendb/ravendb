namespace Rachis.Messages
{
	public class RequestVoteRequest : BaseMessage
	{
		public long Term { get; set; }
		public long LastLogIndex { get; set; }
		public long LastLogTerm { get; set; }

		public bool TrialOnly { get; set; }
		public bool ForcedElection { get; set; }
	}
}