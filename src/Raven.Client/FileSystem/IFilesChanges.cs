using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.FileSystem
{
    public interface IFilesChanges : IConnectableChanges<IFilesChanges>
    {
        IObservableWithTask<ConfigurationChange> ForConfiguration();
        IObservableWithTask<ConflictChange> ForConflicts();
        IObservableWithTask<FileChange> ForFolder(string folder);
        IObservableWithTask<SynchronizationUpdateChange> ForSynchronization();
    }
}
