using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Plugins;
using Raven.Database.Bundles.Replication.Plugins;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Storage;
using Raven.Json.Linq;

namespace Raven.Database
{
    public static class DocumentConflictResolver
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        public static void ResolveConflict(this DocumentDatabase database, JsonDocument document, Storage.IStorageActionsAccessor actions, out JsonDocument newDocument)
        {

            newDocument = null;
            if (document == null)
            {
                return;
            }
            
            var conflicts = actions
                .Documents
                .GetDocumentsWithIdStartingWith(document.Key, 0, int.MaxValue, null)
                .Where(x => x != null && x.Key.Contains("/conflicts/"))
                .ToList();

            KeyValuePair<JsonDocument, DateTime> local;
            KeyValuePair<JsonDocument, DateTime> remote;
            database.GetConflictDocuments(conflicts, out local, out remote);

            var docsReplicationConflictResolvers = database.DocsConflictResolvers();

            foreach (var replicationConflictResolver in docsReplicationConflictResolvers)
            {
                if (remote.Key != null && local.Key != null)
                {
                    RavenJObject metadataToSave;
                    RavenJObject documentToSave;
                    Func<object, JsonDocument> getDocument = key => actions.Documents.DocumentByKey(document.Key);
                    var conflictResolved = replicationConflictResolver.TryResolveConflict(document.Key, 
                        remote.Key.Metadata, 
                        remote.Key.DataAsJson, 
                        local.Key, 
                        getDocument, 
                        out metadataToSave, 
                        out documentToSave);

                    if (conflictResolved)
                    {
                        
                        foreach (var conflict in conflicts)
                        {
                            Etag etag;
                            RavenJObject metadata;
                            actions.Documents.DeleteDocument(conflict.Key, null, out metadata, out etag);
                        }

                        if (metadataToSave != null && metadataToSave.Value<bool>(Constants.RavenDeleteMarker))
                        {
                            database.Documents.Delete(document.Key, null, null);
                        }
                        else
                        {
                            using (database.DocumentLock.Lock())
                            {
                                if (metadataToSave != null)
                                {
                                    metadataToSave.Remove(Constants.RavenReplicationConflictDocument);
                                    metadataToSave.Remove(Constants.RavenReplicationConflict);
                                    metadataToSave.Add("@id", document.Key);
                                }

                                var addDocumentResult = actions.Documents.AddDocument(document.Key, document.Etag, documentToSave, metadataToSave);

                                newDocument = new JsonDocument
                                {
                                    Metadata = metadataToSave,
                                    Key = document.Key,
                                    DataAsJson = documentToSave,
                                    Etag = addDocumentResult.Etag,
                                    LastModified = addDocumentResult.SavedAt,
                                    SkipDeleteFromIndex = addDocumentResult.Updated == false
                                };
                            }
                        }
                    }
                }
            }
        }

        public static ReplicationConfig GetReplicationConfig(this DocumentDatabase database)
        {
            try
            {
                var configDoc = database.ConfigurationRetriever.GetConfigurationDocument<ReplicationConfig>(Constants.RavenReplicationConfig);
                return configDoc?.MergedDocument;
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not read the configuration for replication conflict resolution", e);
                database.AddAlert(new Alert
                {
                    AlertLevel = AlertLevel.Error,
                    CreatedAt = SystemTime.UtcNow,
                    Message = e.Message,
                    Title = "Could not read the configuration for replication conflict resolution",
                    Exception = e.ToString(),
                    UniqueKey = "Replication Conflict Resolution Configuration Error"
                });
                return null;
            }
        }

        public static IEnumerable<AbstractDocumentReplicationConflictResolver> DocsConflictResolvers(this DocumentDatabase database)
        {
            var exported = database.Configuration.Container.GetExportedValues<AbstractDocumentReplicationConflictResolver>();

            var config = database.GetReplicationConfig();

            if (config == null || config.DocumentConflictResolution == StraightforwardConflictResolution.None)
                return exported;

            var withConfiguredResolvers = exported.ToList();

            switch (config.DocumentConflictResolution)
            {
                case StraightforwardConflictResolution.ResolveToLocal:
                    withConfiguredResolvers.Add(LocalDocumentReplicationConflictResolver.Instance);
                    break;
                case StraightforwardConflictResolution.ResolveToRemote:
                    withConfiguredResolvers.Add(RemoteDocumentReplicationConflictResolver.Instance);
                    break;
                case StraightforwardConflictResolution.ResolveToLatest:
                    withConfiguredResolvers.Add(LatestDocumentReplicationConflictResolver.Instance);
                    break;
                default:
                    throw new ArgumentOutOfRangeException("config.DocumentConflictResolution");
            }

            return withConfiguredResolvers;
        }

        public static void GetConflictDocuments(this DocumentDatabase database, IEnumerable<JsonDocument> conflicts, out KeyValuePair<JsonDocument, DateTime> local, out KeyValuePair<JsonDocument, DateTime> remote)
        {
            DateTime localModified = DateTime.MinValue, remoteModified = DateTime.MinValue;
            JsonDocument localDocument = null, newestRemote = null;
            foreach (var conflict in conflicts)
            {
                var lastModified = conflict.LastModified.HasValue ? conflict.LastModified.Value : DateTime.MinValue;
                var replicationSource = conflict.Metadata.Value<string>(Constants.RavenReplicationSource);

                if (string.Equals(replicationSource, database.TransactionalStorage.Id.ToString(), StringComparison.OrdinalIgnoreCase))
                {
                    localModified = lastModified;
                    localDocument = conflict;
                    continue;
                }

                if (lastModified <= remoteModified)
                    continue;

                newestRemote = conflict;
                remoteModified = lastModified;
            }

            local = new KeyValuePair<JsonDocument, DateTime>(localDocument, localModified);
            remote = new KeyValuePair<JsonDocument, DateTime>(newestRemote, remoteModified);
        }
    }
}