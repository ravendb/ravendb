using System;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS.Changes
{
	public interface IServerNotifications
	{
		Task ConnectionTask { get; }
		Task WhenSubscriptionsActive();
		IObservable<ConfigurationChangeNotification> ConfigurationChanges();
		IObservable<ConflictNotification> Conflicts();
		IObservable<FileChangeNotification> FolderChanges(string folder);
		IObservable<SynchronizationUpdateNotification> SynchronizationUpdates();
	}
}
