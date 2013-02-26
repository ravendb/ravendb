using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;

namespace Raven.ClusterManager.Discovery
{
	///<summary>
	/// Publish the presence of a client over the network
	///</summary>
	public class ClusterDiscoveryClient : IDisposable
	{
		private readonly TimeSpan publishLimit = TimeSpan.FromMinutes(5);
		private readonly byte[] buffer;
		private readonly UdpClient udpClient;
		private readonly IPEndPoint allHostsGroup;
		private DateTime lastPublish;

		public ClusterDiscoveryClient(Guid senderId, string clusterManagerUrl)
		{
			buffer = Encoding.UTF8.GetBytes(senderId.ToString() + Environment.NewLine + clusterManagerUrl);
			udpClient = new UdpClient
			{
				ExclusiveAddressUse = false
			};
			allHostsGroup = new IPEndPoint(IPAddress.Parse("224.0.0.1"), 12392);
		}

		///<summary>
		/// Publish the presence of this node
		///</summary>
		public void PublishMyPresence()
		{
			if ((SystemTime.UtcNow - lastPublish) < publishLimit)
				return;
			// avoid a ping storm when we re-publish because we discovered another client
			lock (this)
			{
				if ((SystemTime.UtcNow - lastPublish) < publishLimit)
					return;

				lastPublish = SystemTime.UtcNow;
			}

			Task.Factory.FromAsync<byte[], int, IPEndPoint, int>(udpClient.BeginSend, udpClient.EndSend, buffer, buffer.Length, allHostsGroup, null)
				.ContinueWith(task =>
				{
#pragma warning disable 0219
					var _ = task.Exception;
#pragma warning restore 0219
					// basically just ignoring this error
				});
		}

		void IDisposable.Dispose()
		{
			udpClient.Close();
		}
	}
}