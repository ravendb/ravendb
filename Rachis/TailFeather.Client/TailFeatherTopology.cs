using Rachis.Transport;

namespace TailFeather.Client
{
	public class TailFeatherTopology
	{
		public string CurrentLeader { get; set; }

		public long CurrentTerm { get; set; }

		public long CommitIndex { get; set; }

		public NodeConnectionInfo[] AllVotingNodes { get; set; }

		public NodeConnectionInfo[] PromotableNodes { get; set; }

		public NodeConnectionInfo[] NonVotingNodes { get; set; }

	}
}