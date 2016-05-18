using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Server.Json;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;

namespace Raven.Server.Documents.Replication
{
    //TODO: add code to handle DocumentReplicationStatistics from each replication executer (also aggregation code?)
    //TODO: add support to destinations changes, so they can be changed dynamically (added/removed)
    public class DocumentReplicationLoader : BaseReplicationLoader
    {
        private ReplicationDocument _replicationDocument;
        private readonly ConcurrentSet<Guid> _activeConnections = new ConcurrentSet<Guid>(); 
        private const int MaxSupportedReplicationDestinations = int.MaxValue; //TODO: limit it or make it configurable?

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
            {
                context.OpenReadTransaction();

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
                    _log.Error("failed to deserialize replication configuration document. This is something that is not supposed to happen. Reason:" + e);
                }	           
            }
        }

        //inbound replication source will get it's ReplicationExecuter as well as
        //the outgoing ones
        public DocumentReplicationExecuter RegisterNewConnectionFrom(
            Guid srcDbId, 
            string srcUrl, 
            string srcDbName, 
            long lastSentEtag, 
            out bool shouldConnectBack)
        {
            shouldConnectBack = false;

            //prevent initiating two connections to the same db
            if (!_activeConnections.TryAdd(srcDbId))
                return null;

            var destination = _replicationDocument.Destinations
                .FirstOrDefault(x => x.Url.Equals(srcUrl, StringComparison.OrdinalIgnoreCase) &&
                                     x.Database.Equals(srcDbName, StringComparison.OrdinalIgnoreCase));

            if (destination != null)
                shouldConnectBack = true;

            var documentReplicationExecuter = new DocumentReplicationExecuter(_database, srcDbId, destination, lastSentEtag);
            Replications.Add(documentReplicationExecuter);
            return documentReplicationExecuter;
        }
    }
}
