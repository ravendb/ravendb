using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.Changes;
using System;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Changes
{
    public class FilesConnectionState : ConnectionStateBase
    {
        private readonly Func<FilesConnectionState, Task> ensureConnection;

        public FilesConnectionState(Func<Task> disconnectAction, Func<FilesConnectionState, Task> ensureConnection, Task task)
            : base(disconnectAction, task)
        {
            this.ensureConnection = ensureConnection;
        }

        protected override Task EnsureConnection()
        {
            return ensureConnection(this);
        }

        public event Action<ConfigurationChangeNotification> OnConfigurationChangeNotification = (x) => { };
        public void Send(ConfigurationChangeNotification configurationChangeNotification)
        {
            var onConfigurationChangeNotification = OnConfigurationChangeNotification;
            if (onConfigurationChangeNotification != null)
                onConfigurationChangeNotification(configurationChangeNotification);
        }

        public event Action<ConflictNotification> OnConflictsNotification = (x) => { };
        public void Send(ConflictNotification conflictsNotification)
        {
            var onConflictNotification = OnConflictsNotification;
            if (onConflictNotification != null)
                onConflictNotification(conflictsNotification);
        }

        public event Action<SynchronizationUpdateNotification> OnSynchronizationNotification = (x) => { };
        public void Send(SynchronizationUpdateNotification synchronizationNotification)
        {
            var onSynchronizationNotification = OnSynchronizationNotification;
            if (onSynchronizationNotification != null)
                onSynchronizationNotification(synchronizationNotification);
        }

        public event Action<FileChangeNotification> OnFileChangeNotification = (x) => { };
        public void Send(FileChangeNotification fileChangeNotification)
        {
            var onFileChangeNotification = OnFileChangeNotification;
            if (onFileChangeNotification != null)
                onFileChangeNotification(fileChangeNotification);
        }
    }
}
