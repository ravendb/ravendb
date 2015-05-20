using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Notifications
{
	public interface INotificationPublisher
	{
		void Publish(FileSystemNotification change);
	}
}