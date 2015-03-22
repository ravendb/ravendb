using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.FileSystem
{
    public interface IFilesChanges : IConnectableChanges<IFilesChanges>
    {
        IObservableWithTask<ConfigurationChangeNotification> ForConfiguration();
        IObservableWithTask<ConflictNotification> ForConflicts();
        IObservableWithTask<FileChangeNotification> ForFolder(string folder);
        IObservableWithTask<SynchronizationUpdateNotification> ForSynchronization();
    }
}
