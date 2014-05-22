using Raven.Client.Changes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS.Changes
{
    public interface IFileSystemChanges : IConnectableChanges<IFileSystemChanges>
    {
        IObservableWithTask<ConfigurationChangeNotification> ForConfiguration();
        IObservableWithTask<ConflictNotification> ForConflicts();
        IObservableWithTask<FileChangeNotification> ForFolder(string folder);
        IObservableWithTask<CancellationNotification> ForCancellations();
        IObservableWithTask<SynchronizationUpdateNotification> ForSynchronization();
    }
}
