using System;
using System.Linq;
using System.Threading;
using Jint.Native;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Actions;
using Raven.Database.Config.Retriever;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Impl
{
    internal class DocumentsLeftToReplicate
    {
        private readonly DocumentDatabase database;

        private readonly ReplicationTask replicationTask;

        private readonly HttpRavenRequestFactory requestFactory;
        
        private const int MaxDocumentsToCheck = 25000;

        public string DatabaseId { get; }

        public DocumentsLeftToReplicate(DocumentDatabase database)
        {
            this.database = database;
            DatabaseId = database.TransactionalStorage.Id.ToString();
            requestFactory = new HttpRavenRequestFactory();

            replicationTask = database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
            if (replicationTask == null)
            {
                throw new InvalidOperationException("Couldn't locate ReplicationTask");
            }
        }

        public DocumentCount Calculate(ServerInfo serverInfo)
        {
            var replicationDocument = GetReplicationDocument();

            if (serverInfo.SourceId != DatabaseId)
            {
                return GetDocumentsLeftCountFromAnotherSourceServer(replicationDocument, serverInfo);
            }

            var replicationDestination = replicationDocument
                .Destinations
                .FirstOrDefault(x => FetchTargetServerUrl(x).Equals(serverInfo.DestinationUrl, StringComparison.CurrentCultureIgnoreCase) &&
                            x.Database.Equals(serverInfo.DatabaseName, StringComparison.CurrentCultureIgnoreCase));

            if (replicationDestination == null)
            {
                throw new InvalidOperationException($"Couldn't find replication destination for url: {serverInfo.DestinationUrl} and database: {serverInfo.DatabaseName}");
            }

            var replicationStrategy = ReplicationTask.GetConnectionOptions(replicationDestination, database);
            if (replicationStrategy.IsETL == false)
            {
                return GetDocumentsLeftCount(replicationStrategy, serverInfo.DestinationUrl, serverInfo.DatabaseName);
            }

            return GetDocumentsLeftCountForEtl(replicationStrategy, serverInfo.DestinationUrl, serverInfo.DatabaseName);
        }

        private ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> GetReplicationDocument()
        {
            ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> configurationDocument = null;
            try
            {
                configurationDocument = database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Couldn't find Raven/Replication/Destinations document", e);
            }

            if (configurationDocument == null)
            {
                throw new InvalidOperationException("Couldn't find Raven/Replication/Destinations document");
            }

            var replicationDocument = configurationDocument.MergedDocument;
            return replicationDocument;
        }

        private string FetchTargetServerUrl(ReplicationDestination replicationDestination)
        {
            var url = $"{replicationDestination.Url}/debug/config";

            try
            {
                var replicationStrategy = ReplicationTask.GetConnectionOptions(replicationDestination, database);
                var request = requestFactory.Create(url, HttpMethods.Get, replicationStrategy.ConnectionStringOptions);
                var ravenConfig = request.ExecuteRequest<RavenJObject>();
                var serverUrlFromTargetConfig = ravenConfig.Value<string>("ServerUrl");

                // replace host name with target hostname
                return new UriBuilder(replicationDestination.Url) { Host = new Uri(serverUrlFromTargetConfig).Host }.Uri.ToString();
            }
            catch (Exception e)
            {
                if (replicationDestination.Url.EndsWith("/"))
                    return replicationDestination.Url;

                return replicationDestination.Url + "/";
            }
        }

        private DocumentCount GetDocumentsLeftCountFromAnotherSourceServer(
            ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> replicationDocument, 
            ServerInfo serverInfo)
        {
            var replicationDestination = replicationDocument
                .Destinations
                .FirstOrDefault(x => FetchTargetServerUrl(x).Equals(serverInfo.SourceUrl, StringComparison.CurrentCultureIgnoreCase) &&
                            x.Database.Equals(serverInfo.DatabaseName, StringComparison.CurrentCultureIgnoreCase));

            if (replicationDestination != null)
            {
                return DocumentsLeftCountFromAnotherSourceServer(serverInfo, replicationDestination);
            }

            if (serverInfo.SourcesToIgnore.Contains(DatabaseId) == false)
            {
                serverInfo.SourcesToIgnore.Add(DatabaseId);
                //couldn't find replication destination for this url,
                //going to try to do it through other destinations

                foreach (var destination in replicationDocument.Destinations)
                {
                    try
                    {
                        return DocumentsLeftCountFromAnotherSourceServer(serverInfo, destination);
                    }
                    catch (Exception e)
                    {
                        //couldn't reach this destination through this one, will continue
                    }
                }
            }
            
            throw new InvalidOperationException($"Couldn't find replication destination for url: {serverInfo.SourceUrl} and database: {serverInfo.DatabaseName}");
        }

        private DocumentCount DocumentsLeftCountFromAnotherSourceServer(ServerInfo serverInfo, 
            ReplicationDestination.ReplicationDestinationWithConfigurationOrigin replicationDestination)
        {
            var replicationStrategy = ReplicationTask.GetConnectionOptions(replicationDestination, database);
            var url = $"{replicationStrategy.ConnectionStringOptions.Url}/admin/replication/docs-left-to-replicate";
            var request = requestFactory.Create(url, HttpMethods.Post, replicationStrategy.ConnectionStringOptions);
            request.Write(RavenJObject.FromObject(serverInfo));

            var documentsLeftCount = request.ExecuteRequest<DocumentCount>();
            return documentsLeftCount;
        }

        private DocumentCount GetDocumentsLeftCount(ReplicationStrategy replicationStrategy, 
            string destinationUrl, string databaseName)
        {
            //first check the stats
            var localDocumentCount = database.Statistics.CountOfDocuments;
            var url = $"{replicationStrategy.ConnectionStringOptions.Url}/stats";
            var request = requestFactory.Create(url, HttpMethods.Get, replicationStrategy.ConnectionStringOptions);
            var remoteDocumentCount = request.ExecuteRequest<DatabaseStatistics>().CountOfDocuments;
            var difference = localDocumentCount - remoteDocumentCount;
            if (difference > MaxDocumentsToCheck)
            {
                return new DocumentCount
                {
                    Count = difference,
                    Type = CountType.Approximate,
                    IsEtl = false
                };
            }

            var sourcesDocument = replicationTask.GetLastReplicatedEtagFrom(replicationStrategy);
            if (sourcesDocument == null)
            {
                throw new InvalidOperationException($"Couldn't get last replicated etag for destination url: {destinationUrl} and database: {databaseName}");
            }

            long count = 0;
            var earlyExit = new Reference<bool>();
            database.TransactionalStorage.Batch(actions =>
            {
                //get document count since last replicated etag
                var destinationId = sourcesDocument.ServerInstanceId.ToString();
                count = actions.Documents.GetDocumentIdsAfterEtag(
                    sourcesDocument.LastDocumentEtag, MaxDocumentsToCheck,
                    WillDocumentBeReplicated(replicationStrategy, destinationId), earlyExit,
                    database.WorkContext.CancellationToken).Count();
            });

            return new DocumentCount
            {
                Count = earlyExit.Value == false ? count : Math.Max(count, difference),
                Type = earlyExit.Value == false ? CountType.Accurate : CountType.Approximate,
                IsEtl = false
            };
        }

        private Func<string, RavenJObject, Func<JsonDocument>, bool> WillDocumentBeReplicated(
            ReplicationStrategy strategy, string destinationId, Action skippedAction = null)
        {
            return (key, metadata, getDocument) =>
            {
                string _;
                if (strategy.FilterDocuments(destinationId, key, metadata, out _) == false)
                {
                    skippedAction?.Invoke();
                    return false;
                }

                if (strategy.IsETL == false)
                {
                    //not ETL replication
                    return true;
                }

                var collection = metadata.Value<string>(Constants.RavenEntityName);
                string script;
                if (string.IsNullOrEmpty(collection) ||
                    strategy.SpecifiedCollections.TryGetValue(collection, out script) == false)
                {
                    //document has no collection or this collection will not be replicated
                    skippedAction?.Invoke();
                    return false;
                }

                if (string.IsNullOrEmpty(script) || metadata.ContainsKey(Constants.RavenDeleteMarker))
                {
                    //no filter script or this document is deleted
                    return true;
                }

                if (WillReplicateByScript(script, getDocument()) == false)
                {
                    skippedAction?.Invoke();
                    return false;
                }

                return true;
            };
        }

        private bool WillReplicateByScript(string script, JsonDocument document)
        {
            var scriptedPatchRequest = new ScriptedPatchRequest
            {
                Script = script
            };

            var patcher = new ReplicationScriptedJsonPatcher(database, scriptedPatchRequest);
            using (var scope = new DefaultScriptedJsonPatcherOperationScope(database))
            {
                try
                {
                    patcher.Apply(scope, document.ToJson(), scriptedPatchRequest, document.SerializedSizeOnDisk);
                    //null means that we should skip this document
                    return scope.ActualPatchResult != JsValue.Null;
                }
                catch (Exception e)
                {
                    //this document will not be replicated
                    return false;
                }
            }
        }

        private DocumentCount GetDocumentsLeftCountForEtl(ReplicationStrategy replicationStrategy, string destinationUrl, string databaseName)
        {
            var sourcesDocument = replicationTask.GetLastReplicatedEtagFrom(replicationStrategy);
            if (sourcesDocument == null)
            {
                throw new InvalidOperationException($"Couldn't get last replicated etag for destination url: {destinationUrl} and database: {databaseName}");
            }

            long storageCount = 0;
            var earlyExit = new Reference<bool>();
            database.TransactionalStorage.Batch(actions =>
            {
                //get document count since last replicated etag
                long skipped = 0;
                var destinationId = sourcesDocument.ServerInstanceId.ToString();
                storageCount = actions.Documents.GetDocumentIdsAfterEtag(
                    sourcesDocument.LastDocumentEtag, MaxDocumentsToCheck,
                    WillDocumentBeReplicated(replicationStrategy, destinationId), 
                    earlyExit, database.WorkContext.CancellationToken).Count();
            });

            long diffFromIndex = 0;
            if (earlyExit.Value)
            {
                //we couldn't get an accurate document left count from the storage,
                //we'll try to get an approximation from the index
                var entityNames = replicationStrategy.SpecifiedCollections.Keys.ToHashSet();
                var query = QueryBuilder.GetQueryForAllMatchingDocumentsForIndex(database, entityNames);

                //get count of documents with specified collections on this server
                var localDocumentCount = GetDocumentCountForEntityNames(query);

                //get count of documents with specified collections on destination server
                var url = $"{replicationStrategy.ConnectionStringOptions.Url}/admin/replication/replicated-docs-by-entity-names";
                var request = requestFactory.Create(url, HttpMethods.Post, replicationStrategy.ConnectionStringOptions);
                request.Write(query);
                var remoteDocumentCount = request.ExecuteRequest<long>();

                diffFromIndex = localDocumentCount - remoteDocumentCount;
            }

            return new DocumentCount
            {
                Count = earlyExit.Value == false ? storageCount : Math.Max(storageCount, diffFromIndex),
                Type = earlyExit.Value == false ? CountType.Accurate : CountType.Approximate,
                IsEtl = true
            };
        }

        public long GetDocumentCountForEntityNames(string query)
        {
            long localDocumentCount = 0;

            database.TransactionalStorage.Batch(actions =>
            {
                using (var internalCts = new CancellationTokenSource())
                using (var linked = CancellationTokenSource.CreateLinkedTokenSource(database.WorkContext.CancellationToken, internalCts.Token))
                using (var op = new QueryActions.DatabaseQueryOperation(database, Constants.DocumentsByEntityNameIndex, new IndexQuery
                {
                    Query = query,
                    PageSize = int.MaxValue
                }, actions, linked)
                {
                    ShouldSkipDuplicateChecking = true
                })
                {
                    op.Init();
                    localDocumentCount = op.Header.TotalResults;
                }
            });

            return localDocumentCount;
        }

        public void ExtractDocumentIds(ServerInfo serverInfo, Action<string> successAction, Action skippedAction)
        {
            var replicationDocument = GetReplicationDocument();

            var replicationDestination = replicationDocument
                .Destinations
                .FirstOrDefault(x => FetchTargetServerUrl(x).Equals(serverInfo.DestinationUrl, StringComparison.CurrentCultureIgnoreCase) &&
                            x.Database.Equals(serverInfo.DatabaseName, StringComparison.CurrentCultureIgnoreCase));

            if (replicationDestination == null)
            {
                throw new InvalidOperationException($"Couldn't find replication destination for url: {serverInfo.DestinationUrl} and database: {serverInfo.DatabaseName}");
            }

            var replicationStrategy = ReplicationTask.GetConnectionOptions(replicationDestination, database);
            var sourcesDocument = replicationTask.GetLastReplicatedEtagFrom(replicationStrategy);
            if (sourcesDocument == null)
            {
                throw new InvalidOperationException($"Couldn't get last replicated etag for " +
                                                    $"destination url: {serverInfo.DestinationUrl} and database: {serverInfo.DatabaseName}");
            }

            database.TransactionalStorage.Batch(actions =>
            {
                var earlyExit = new Reference<bool>();

                //get document count since last replicated etag
                var documentIds = actions.Documents.GetDocumentIdsAfterEtag(
                    sourcesDocument.LastDocumentEtag, int.MaxValue,
                    WillDocumentBeReplicated(replicationStrategy, sourcesDocument.ServerInstanceId.ToString(), skippedAction), 
                    earlyExit, database.WorkContext.CancellationToken);

                foreach (var documentId in documentIds)
                {
                    successAction(documentId);
                }
            });
        }
    }

    public class DocumentCount
    {
        public DocumentCount()
        {
            IsEtl = false;
        }

        public long Count { get; set; }

        public CountType Type { get; set; }

        public bool IsEtl { get; set; }
    }

    public enum CountType
    {
        Accurate,
        Approximate
    }
}
