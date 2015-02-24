using Rachis.Storage;

namespace Rachis.Messages
{
	public class InstallSnapshotRequest : BaseMessage
	{
		public long Term { get; set; }

		public long LastIncludedIndex { get; set; }

		public long LastIncludedTerm { get; set; }

		public Topology Topology { get; set; }
	}
}
