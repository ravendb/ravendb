using System;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS.Changes
{
	public interface IServerNotifications
	{
		Task ConnectionTask { get; }
		Task WhenSubscriptionsActive();
		IObservable<ConfigChange> ConfigurationChanges();
		IObservable<ConflictNotification> Conflicts();
		IObservable<FileChange> FolderChanges(string folder);
		IObservable<SynchronizationUpdate> SynchronizationUpdates();
	}
}
