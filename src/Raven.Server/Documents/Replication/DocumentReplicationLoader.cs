using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Replication
{
    //TODO: add code to handle DocumentReplicationStatistics from each replication executer (also aggregation code?)
    public class DocumentReplicationLoader : BaseReplicationLoader
    {
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

                var configuration = JsonDeserialization.DocumentReplicationConfiguration(configurationDocument.Data);				
                //TODO: make sure that destinations are unique (check uniqueness for urls?)
                //if there are destinations with non-unique urls, use the first instance
                //also, if there are destinations with multiple non-unique urls, add relevant alert

                //something like : 
                //				_database.AddAlert(new Alert
                //				{
                //					Title = "Multiple non-unique destinations are configured. Using the first one.",
                //					CreatedAt = DateTime.UtcNow
                //				});

                if (configuration.Destinations == null) //precaution, should not happen
                {
                    _log.Warn("Invalid configuration document, Destinations property must not be null. Replication will not be active");
                    return;
                }

                foreach (var destinationConfig in configuration.Destinations.Take(MaxSupportedReplicationDestinations))
                {					
                    var replicationExecuter = new DocumentReplicationExecuter(_database, destinationConfig);
                    Replications.Add(replicationExecuter);
                    replicationExecuter.Start();
                }
            }
        }
    }
}
