using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.ReplicationUtil
{
    public abstract class BaseReplicationLoader : IDisposable
    {
        protected readonly Logger _logger;
        protected readonly DocumentDatabase _database;
        public readonly ConcurrentSet<BaseReplicationExecuter> Replications = new ConcurrentSet<BaseReplicationExecuter>();

        protected BaseReplicationLoader(DocumentDatabase database)
        {
            _database = database;
            _logger = LoggerSetup.Instance.GetLogger(_database.Name, GetType().FullName);
            _database.Notifications.OnDocumentChange += WakeReplication;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
        }

        private void WakeReplication(DocumentChangeNotification documentChangeNotification)
        {
            foreach (var replication in Replications)
                replication.WaitForChanges.SetByAsyncCompletion();
        }

        protected abstract bool ShouldReloadConfiguration(string systemDocumentKey);

        protected abstract void LoadConfigurations();

        private void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (ShouldReloadConfiguration(notification.Key))
            {
                foreach (var replication in Replications)
                    replication.Dispose();

                Replications.Clear();
                LoadConfigurations();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Replication configuration was changed: {notification.Key}");
            }
        }

        public void Initialize()
        {
            LoadConfigurations();
        }

        public virtual void Dispose()
        {
            _database.Notifications.OnDocumentChange -= WakeReplication;
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }
    }
}