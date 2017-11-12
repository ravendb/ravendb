using System;
using System.Collections.Generic;
using System.Linq;
using System.Management.Automation;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Data;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Powershell
{
    [Cmdlet(VerbsData.Restore, "Database")]
    public class BinaryRestoreDatabaseInClusterCmdlet : Cmdlet
    {
        private bool incremental;

        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Url of RavenDB server, including the port. Example --> http://localhost:8080")]
        public string ServerUrl { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Database name in destination RavenDB server")]
        public string DatabaseName { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "ApiKey to use when connecting to RavenDB Server. It should be full API key. Example --> key1/sAdVA0KLqigQu67Dxj7a")]
        public string ApiKey { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "If true, restore from incremental backup. Otherwise, restore from a full backup.")]
        public SwitchParameter Incremental
        {
            get { return incremental; }
            set { incremental = value; }
        }

        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Where to write the backup")]
        public string BackupLocation { get; set; }

        protected override void ProcessRecord()
        {
            using (var store = new DocumentStore
            {
                Url = ServerUrl,
                ApiKey = ApiKey
            })
            {
                store.Initialize();
                
                var globalReplicationDestinationsJson = store.DatabaseCommands.Get(Constants.Global.ReplicationDestinationsDocumentName);
                var globalReplicationDestinations = globalReplicationDestinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
                var restoreOperations = new List<Tuple<Operation, IDocumentStore>>();
                var replicationDestinations = globalReplicationDestinations.Destinations;
                replicationDestinations.Add(new ReplicationDestination()
                {
                    ApiKey = ApiKey,
                    Url = ServerUrl
                });

                foreach (var node in replicationDestinations)
                {
                    var curNodeStore = new DocumentStore
                    {
                        Url = node.Url,
                        ApiKey = node.ApiKey
                    }.Initialize();

                    var restoreOperation = curNodeStore.DatabaseCommands.GlobalAdmin.StartRestore(new DatabaseRestoreRequest
                    {
                        BackupLocation = BackupLocation,
                        DatabaseName = DatabaseName,
                        DisableReplicationDestinations = true,
                        GenerateNewDatabaseId = true
                    });
                    restoreOperations.Add(Tuple.Create(restoreOperation, curNodeStore));
                }

                foreach (var restoreOperation in restoreOperations)
                {
                    restoreOperation.Item1.WaitForCompletion();
                    restoreOperation.Item2.Dispose();
                }

                var databasesStatistics = new Dictionary<string, DatabaseStatistics>();

                replicationDestinations.ForEach(x =>
                {
                    using (var curNodeStore = new DocumentStore
                    {
                        Url = x.Url,
                        ApiKey = x.ApiKey,
                        DefaultDatabase = DatabaseName
                    }.Initialize())
                    {
                        var statistics = curNodeStore.DatabaseCommands.GetStatistics();
                        databasesStatistics[x.Url] = statistics;
                    }
                });

                foreach (var node in replicationDestinations)
                {
                    using (var curNodeStore = new DocumentStore
                    {
                        Url = node.Url,
                        ApiKey = node.ApiKey,
                        DefaultDatabase = DatabaseName
                    }.Initialize())
                    {
                        using (var session = curNodeStore.OpenSession())
                        {
                            var sources = session.Advanced.LoadStartingWith<RavenJObject>("Raven/Replication/Sources/", pageSize: int.MaxValue);
                            foreach (var ravenJObject in sources)
                            {
                                session.Delete(ravenJObject);
                            }
                            session.Delete("Raven/Replication/DatabaseIdsCache");
                            session.SaveChanges();
                        }

                        using (var session = curNodeStore.OpenSession())
                        {
                            foreach (var replicationDestination in replicationDestinations.Where(x => x.Url != node.Url))
                            {
                                var curNodeStatistics = databasesStatistics[replicationDestination.Url];
                                var replicationDestinationUrl = replicationDestination.Url;
                                if (replicationDestinationUrl[replicationDestinationUrl.Length - 1] == '/')
                                {
                                    replicationDestinationUrl = replicationDestinationUrl.Substring(0, replicationDestinationUrl.Length - 1);
                                }
                                replicationDestinationUrl += "/databases/" + DatabaseName;

                                session.Store(new SourceReplicationInformation
                                {
                                    Source = replicationDestinationUrl,
                                    LastDocumentEtag = curNodeStatistics.LastDocEtag,
                                    LastAttachmentEtag = curNodeStatistics.LastAttachmentEtag,
                                    ServerInstanceId = curNodeStatistics.DatabaseId,
                                    LastModified = SystemTime.UtcNow,
                                    LastBatchSize = 0,
                                    LastModifiedAtSource = DateTime.MinValue
                                }, "Raven/Replication/Sources/"+ curNodeStatistics.DatabaseId);
                            }
                            session.SaveChanges();
                        }
                    }
                }

                foreach (var node in replicationDestinations)
                {
                    using (var curNodeStore = new DocumentStore
                    {
                        Url = node.Url,
                        ApiKey = node.ApiKey,
                        DefaultDatabase = DatabaseName
                    }.Initialize())
                    {
                        using (var session = curNodeStore.OpenSession())
                        {
                            var replicationDoc = session.Load<ReplicationDocument>(Constants.RavenReplicationDestinations);
                            if (replicationDoc?.Destinations != null)
                            {
                                foreach (var replicationDocDestination in replicationDoc.Destinations)
                                {
                                    replicationDocDestination.Disabled = false;
                                }
                                session.Store(replicationDoc);
                                session.SaveChanges();
                            }
                        }
                    }
                }
                  
            }
        }
    }
}