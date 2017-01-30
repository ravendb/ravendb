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

        public event Action<ConfigurationChange> OnConfigurationChangeNotification = (x) => { };
        public void Send(ConfigurationChange configurationChange)
        {
            var onConfigurationChangeNotification = OnConfigurationChangeNotification;
            if (onConfigurationChangeNotification != null)
                onConfigurationChangeNotification(configurationChange);
        }

        public event Action<ConflictChange> OnConflictsNotification = (x) => { };
        public void Send(ConflictChange conflictsChange)
        {
            var onConflictNotification = OnConflictsNotification;
            if (onConflictNotification != null)
                onConflictNotification(conflictsChange);
        }

        public event Action<SynchronizationUpdateChange> OnSynchronizationNotification = (x) => { };
        public void Send(SynchronizationUpdateChange synchronizationChange)
        {
            var onSynchronizationNotification = OnSynchronizationNotification;
            if (onSynchronizationNotification != null)
                onSynchronizationNotification(synchronizationChange);
        }

        public event Action<FileChange> OnFileChangeNotification = (x) => { };
        public void Send(FileChange fileChange)
        {
            var onFileChangeNotification = OnFileChangeNotification;
            if (onFileChangeNotification != null)
                onFileChangeNotification(fileChange);
        }
    }
}
