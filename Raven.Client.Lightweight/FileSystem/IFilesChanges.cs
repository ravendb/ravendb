using Raven.Client.Changes;
using Raven.Client.RavenFS;
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
}
