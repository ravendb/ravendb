using Raven.Abstractions.FileSystem.Notifications;
using Raven.NewClient.Client.Changes;

namespace Raven.NewClient.Client.FileSystem
{
    public interface IFilesChanges : IConnectableChanges<IFilesChanges>
    {
        IObservableWithTask<ConfigurationChangeNotification> ForConfiguration();
        IObservableWithTask<ConflictNotification> ForConflicts();
        IObservableWithTask<FileChangeNotification> ForFolder(string folder);
        IObservableWithTask<SynchronizationUpdateNotification> ForSynchronization();
    }
}
