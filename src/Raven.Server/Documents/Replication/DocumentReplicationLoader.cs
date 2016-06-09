using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Replication
{
    //TODO: add code to handle DocumentReplicationStatistics from each replication executer (also aggregation code?)
    //TODO: add support to destinations changes, so they can be changed dynamically (added/removed)
    public class DocumentReplicationLoader
    {
        private readonly ILog _log;
        private readonly DocumentDatabase _database;

        private ReplicationDocument _replicationDocument;
        private readonly List<OutgoingDocumentReplication> _outgoingReplications;

        public DocumentReplicationLoader(DocumentDatabase database) 
        {
            _outgoingReplications = new List<OutgoingDocumentReplication>();
            _database = database;
            _log = LogManager.GetLogger(GetType());
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
            ReplicationUniqueName = $"{_database.Name} -> {_database.DbId}";
        }

        public string ReplicationUniqueName { get; }

        public void Initialize()
        {
            LoadConfigurations();
        }

        protected bool ShouldReloadConfiguration(string systemDocumentKey)
        {
            return systemDocumentKey.Equals(Constants.Replication.DocumentReplicationConfiguration,
                StringComparison.OrdinalIgnoreCase);
        }

        protected void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var configurationDocument = _database.DocumentsStorage.Get(context,
                    Constants.Replication.DocumentReplicationConfiguration);

                if (configurationDocument == null)
                    return;

                try
                {
                    _replicationDocument = JsonDeserialization.ReplicationDocument(configurationDocument.Data);
                    //the destinations here are the ones that are outbound..
                    if (_replicationDocument.Destinations == null) //precaution, should not happen
                        _log.Warn("Invalid configuration document, Destinations property must not be null. Replication will not be active");
                }
                catch (Exception e)
                {
                    _log.Error("failed to deserialize replication configuration document. This is something that is not supposed to happen. Reason:" + e);
                }

                Debug.Assert(_replicationDocument.Destinations != null);
                OnConfigurationChanged(_replicationDocument.Destinations);
            }
        }

        //TODO: add here error handling for the following cases
        //1) what if unexpected exception happens in outgoing replication dispose?
        //2) what if sending a replication batch is happening during a call to dispose?
        protected void OnConfigurationChanged(List<ReplicationDestination> destinations)
        {
            lock (_outgoingReplications)
            {
                foreach (var replication in _outgoingReplications)
                    replication.Dispose();
                _outgoingReplications.Clear();

                var initializationTasks = new List<Task>();
                foreach (var dest in destinations)
                {
                    var outgoingDocumentReplication = new OutgoingDocumentReplication(_database, dest);
                    initializationTasks.Add(outgoingDocumentReplication.InitializeAsync());
                    _outgoingReplications.Add(outgoingDocumentReplication);
                }

                Task.WhenAll(initializationTasks).Wait(_database.DatabaseShutdown);
            }
        }

        public void Dispose()
        {
            lock (_outgoingReplications)
            {
                foreach (var replication in _outgoingReplications)
                    replication.Dispose();
            }
            _database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }

        private void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (ShouldReloadConfiguration(notification.Key))
            {
                LoadConfigurations();

                if (_log.IsDebugEnabled)
                    _log.Debug($"Replication configuration was changed: {notification.Key}");
            }
        }
    }
}
