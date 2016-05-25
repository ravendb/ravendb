using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Server.Json;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Replication
{
    //TODO: add code to handle DocumentReplicationStatistics from each replication executer (also aggregation code?)
    //TODO: add support to destinations changes, so they can be changed dynamically (added/removed)
    public class DocumentReplicationLoader : BaseReplicationLoader
    {
        private ReplicationDocument _replicationDocument;        

        public DocumentReplicationLoader(DocumentDatabase database) : base(database)
        {
        }

        protected override bool ShouldReloadConfiguration(string systemDocumentKey)
        {
            return systemDocumentKey.Equals(Constants.DocumentReplication.DocumentReplicationConfiguration,
                StringComparison.OrdinalIgnoreCase);
        }      

        protected override void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var configurationDocument = _database.DocumentsStorage.Get(context,
                    Constants.DocumentReplication.DocumentReplicationConfiguration);

                if (configurationDocument == null)
                    return;

                try
                {
                    _replicationDocument = JsonDeserialization.ReplicationDocument(configurationDocument.Data);
                    //the destinations here are the ones that are outbound..
                    if (_replicationDocument.Destinations == null) //precaution, should not happen
                    {
                        _log.Warn(
                            "Invalid configuration document, Destinations property must not be null. Replication will not be active");
                    }
                }
                catch (Exception e)
                {
                    _log.Error(
                        "failed to deserialize replication configuration document. This is something that is not supposed to happen. Reason:" +
                        e);
                }

                for (int i = 0; i < _replicationDocument.Destinations.Count; i++)
                {
                    var destination = _replicationDocument.Destinations[i];
                    try
                    {
                        bool _;
                        var executer = RegisterConnection(Guid.Empty, destination.Url, destination.Database, out _);
                        if (executer.HasOutgoingReplication)
                            executer.Start();
                    }
                    catch (Exception e)
                    {
                        _log.Error($"Failed to register connection for destination {destination.Url} -> {destination.Database}", e);
                    }
                }
            }
        }

        //inbound replication source will get it's ReplicationExecuter as well as
        //the outgoing ones		
        public DocumentReplicationExecuter RegisterConnection(
            Guid srcDbId, 
            string url, 
            string dbName, 
            out bool shouldConnectBack)
        {
            //since this should be done once per destination node, 
            //the mutext is not likely to be a bottleneck
            lock (Replications)
            {
                shouldConnectBack = false;

                var existingExecuter =
                    Replications.Select(x => x as DocumentReplicationExecuter)
                        .FirstOrDefault(x => x.DbId == srcDbId ||
                                        (x.Url.Equals(url,StringComparison.OrdinalIgnoreCase) &&
                                         x.DbName.Equals(dbName,StringComparison.OrdinalIgnoreCase)));

                if (existingExecuter != null)
                    return existingExecuter;


                ReplicationDestination destination = null;
                if (_replicationDocument?.Destinations != null)
                {
                    destination = _replicationDocument.Destinations
                        .FirstOrDefault(x => x.Url.Equals(url, StringComparison.OrdinalIgnoreCase) &&
                                             x.Database.Equals(dbName, StringComparison.OrdinalIgnoreCase));

                    if (destination != null)
                        shouldConnectBack = true;
                }

                var documentReplicationExecuter = new DocumentReplicationExecuter(_database, url, destination);
                Replications.Add(documentReplicationExecuter);
                return documentReplicationExecuter;
            }
        }

        public void HandleConnectionDisconnection(DocumentReplicationExecuter replicationExecuter)
        {
            lock (Replications)
            {
                replicationExecuter.Dispose();
                Replications.TryRemove(replicationExecuter);
            }
        }
    }
}
