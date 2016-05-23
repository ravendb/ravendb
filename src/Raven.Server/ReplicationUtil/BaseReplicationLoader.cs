using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents;

namespace Raven.Server.ReplicationUtil
{
    public abstract class BaseReplicationLoader : IDisposable
    {
        protected readonly ILog _log;
        protected readonly DocumentDatabase _database;
        public readonly List<BaseReplicationExecuter> Replications = new List<BaseReplicationExecuter>();

        protected BaseReplicationLoader(DocumentDatabase database)
        {
            _database = database;
            _log = LogManager.GetLogger(GetType());
            _database.Notifications.OnDocumentChange += WakeReplication;
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
        }

        private void WakeReplication(DocumentChangeNotification documentChangeNotification)
        {
            foreach (var replication in Replications)
                replication.WaitForChanges.Set();
        }

        protected abstract bool ShouldReloadConfiguration(string systemDocumentKey);

        protected abstract void LoadConfigurations();

        private void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (ShouldReloadConfiguration(notification.Key))
            {
                foreach (var replication in Replications)
                {
                    replication.Dispose();
                }
                Replications.Clear();

                LoadConfigurations();

                if (_log.IsDebugEnabled)
                    _log.Debug(() => $"Replication configuration was changed: {notification.Key}");
            }
        }

        public void Initialize()
        {
            LoadConfigurations();
        }

        public void Dispose()
        {
            _database.Notifications.OnDocumentChange -= WakeReplication;
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }
    }
}