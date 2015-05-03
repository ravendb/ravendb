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

		public void RaiseNotification(CounterChangeNotification change)
		{
			transportState.Send(change);
		}

		public void RaiseNotification(CounterBulkOperationNotification change)
		{
			transportState.Send(change);
		}
	}
}