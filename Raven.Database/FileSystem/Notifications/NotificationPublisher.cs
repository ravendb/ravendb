using Raven.Abstractions.FileSystem;
using Raven.Database.Server.Connections;

namespace Raven.Database.FileSystem.Notifications
{
	public class NotificationPublisher : INotificationPublisher
	{
		private readonly TransportState transportState;

		public NotificationPublisher(TransportState transportState)
		{
			this.transportState = transportState;
		}

		public void Publish(FileSystemNotification change)
		{
			transportState.Send(change);
		}
	}
}