using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Rhino.Licensing.Discovery
{
	///<summary>
	/// Publish the precense of a client over the network
	///</summary>
	public class DiscoveryClient : IDisposable
	{
		private readonly byte[] buffer;
		private readonly UdpClient udpClient;
		private readonly IPEndPoint allHostsGroup;

		///<summary>
		/// Create a new instance
		///</summary>
		public DiscoveryClient(Guid senderId, Guid userId, string machineName, string userName)
		{
			buffer = Encoding.UTF8.GetBytes(senderId + Environment.NewLine + userId + Environment.NewLine + machineName + Environment.NewLine + userName);
			udpClient = new UdpClient
			{
				ExclusiveAddressUse = false
			};
			allHostsGroup = new IPEndPoint(IPAddress.Parse("224.0.0.1"), 12391);
		}

		///<summary>
		/// Publish the presence of this node
		///</summary>
		public void PublishMyPresence()
		{
			Task.Factory.FromAsync<byte[], int, IPEndPoint, int>(udpClient.BeginSend, udpClient.EndSend, buffer, buffer.Length, allHostsGroup, null)
				.ContinueWith(task =>
				{
					var _ = task.Exception;
					// basically just ignoring this error
				});
		}

		void IDisposable.Dispose()
		{
			udpClient.Close();
		}
	}
}