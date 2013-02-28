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
		private readonly byte[] buffer;
		private readonly UdpClient udpClient;
		private readonly IPEndPoint allHostsGroup;

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
		public async Task PublishMyPresenceAsync()
		{
			await udpClient.SendAsync(buffer, buffer.Length, allHostsGroup);
		}

		void IDisposable.Dispose()
		{
			udpClient.Close();
		}
	}
}