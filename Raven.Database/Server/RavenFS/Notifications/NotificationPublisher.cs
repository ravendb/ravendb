using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Infrastructure.Connections;

namespace Raven.Database.Server.RavenFS.Notifications
{
	public class NotificationPublisher : INotificationPublisher
	{
		private readonly TransportState transportState;

		public NotificationPublisher(TransportState transportState)
		{
			this.transportState = transportState;
		}

		public void Publish(Notification change)
		{
			transportState.Send(change);
		}
	}
}