using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Rhino.Licensing.Discovery
{
	///<summary>
	/// Publish the presence of a client over the network
	///</summary>
	public class DiscoveryClient : IDisposable
	{
		private readonly TimeSpan publishLimit;
		private readonly byte[] buffer;
		private readonly UdpClient udpClient;
		private readonly IPEndPoint allHostsGroup;
		private DateTime lastPublish;

		public DiscoveryClient(Guid senderId, Guid userId, string machineName, string userName)
			: this(senderId, userId, machineName, userName, TimeSpan.FromMinutes(5))
		{
		}

		public DiscoveryClient(Guid senderId, Guid userId, string machineName, string userName, TimeSpan publishLimit)
		{
			this.publishLimit = publishLimit;
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
			if ((DateTime.UtcNow - lastPublish) < publishLimit)
				return;
			// avoid a ping storm when we re-publish because we discovered another client
			lock (this)
			{
				if ((DateTime.UtcNow - lastPublish) < publishLimit)
					return;

				lastPublish = DateTime.UtcNow;
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