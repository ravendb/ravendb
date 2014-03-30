using Raven.Client.RavenFS;

namespace Raven.Database.Server.RavenFS.Notifications
{
	public interface INotificationPublisher
	{
		void Publish(Notification change);
	}
}