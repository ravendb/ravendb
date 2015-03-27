using System;

namespace Rachis.Messages
{
	public abstract class BaseMessage
	{
		public string From { get; set; }
		public Guid ClusterTopologyId { get; set; }
	}

	public class DisconnectedFromCluster : BaseMessage
	{
		public long Term { get; set; }
	}

	public class NothingToDo { }
}