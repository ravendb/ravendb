using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.Changes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IFilesChanges : IConnectableChanges<IFilesChanges>
    {
        IObservableWithTask<ConfigurationChangeNotification> ForConfiguration();
        IObservableWithTask<ConflictNotification> ForConflicts();
        IObservableWithTask<FileChangeNotification> ForFolder(string folder);
        IObservableWithTask<CancellationNotification> ForCancellations();
        IObservableWithTask<SynchronizationUpdateNotification> ForSynchronization();
    }

    internal interface IFilesChangesImpl
    {
        void AddObserver(IObserver<ConflictNotification> observer);
    }
}
