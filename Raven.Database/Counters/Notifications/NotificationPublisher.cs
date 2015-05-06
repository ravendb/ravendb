using Raven.Abstractions.Counters.Notifications;
using Raven.Database.Server.Connections;

namespace Raven.Database.Counters.Notifications
{
	public class NotificationPublisher
	{
		private readonly TransportState transportState;

		public NotificationPublisher(TransportState transportState)
		{
			this.transportState = transportState;
		}

		public void RaiseNotification(LocalChangeNotification notification)
		{
			transportState.Send(notification);
		}

		public void RaiseNotification(ReplicationChangeNotification notification)
		{
			transportState.Send(notification);
		}

		public void RaiseNotification(BulkOperationNotification change)
		{
			transportState.Send(change);
		}
	}
}