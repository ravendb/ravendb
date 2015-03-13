using System;
using System.IO;
using System.Threading;
using Rachis.Messages;
using Rachis.Transport;

namespace Rachis.Interfaces
{

	/// <summary>
	/// abstraction for transport between Raft nodes.
	/// </summary>
	public interface ITransport : IDisposable
	{
		bool TryReceiveMessage(int timeout, CancellationToken cancellationToken, out MessageContext messageContext);

		void Stream(NodeConnectionInfo dest, InstallSnapshotRequest snapshotRequest, Action<Stream> streamWriter);

		void Send(NodeConnectionInfo dest, CanInstallSnapshotRequest req);
		void Send(NodeConnectionInfo dest, TimeoutNowRequest req);
		void Send(NodeConnectionInfo dest, DisconnectedFromCluster req);
		void Send(NodeConnectionInfo dest, AppendEntriesRequest req);
		void Send(NodeConnectionInfo dest, RequestVoteRequest req);

		void SendToSelf(AppendEntriesResponse resp);
	}
}