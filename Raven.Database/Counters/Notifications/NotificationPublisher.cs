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

		public void RaiseNotification(ChangeNotification notification)
		{
			transportState.Send(notification);
		}

		public void RaiseNotification(BulkOperationNotification change)
		{
			transportState.Send(change);
		}
	}
}