using Raven.Abstractions.FileSystem.Notifications;
using Raven.Client.Changes;
using System;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Changes
{
    public class FilesConnectionState : IChangesConnectionState
    {
        private readonly Action onZero;
        private readonly Task task;
        private int value;

        public Task Task
        {
            get { return task; }
        }

        public FilesConnectionState(Action onZero, Task task)
        {
            value = 0;
            this.onZero = onZero;
            this.task = task;
        }

        public void Inc()
        {
            lock (this)
            {
                value++;
            }
        }

        public void Dec()
        {
            lock (this)
            {
                if (--value == 0)
                    onZero();
            }
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

        public event Action<Exception> OnError;

        public void Error(Exception e)
        {
            var onOnError = OnError;
            if (onOnError != null)
                onOnError(e);
        }
    }
}
