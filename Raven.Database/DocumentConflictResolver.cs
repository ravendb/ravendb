using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Plugins;
using Raven.Database.Bundles.Replication.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database
{
    public static class DocumentConflictResolver
    {

        public static bool ResolveConflict(this DocumentDatabase database, JsonDocument document)
        {
            if (document == null)
            {
                return false;
            }

            IStorageActionsAccessor actions = database.TransactionalStorage.CreateAccessor();
            if (actions == null) throw new ArgumentNullException(nameof(actions));

            var conflicts = actions
                .Documents
                .GetDocumentsWithIdStartingWith(document.Key, 0, Int32.MaxValue, null)
                .Where(x => x.Key.Contains("/conflicts/"))
                .ToList();

            KeyValuePair<JsonDocument, DateTime> local;
            KeyValuePair<JsonDocument, DateTime> remote;
            database.GetConflictDocuments(conflicts, out local, out remote);

            var docsReplicationConflictResolvers = database.DocsConflictResolvers();
            
            foreach (var replicationConflictResolver in docsReplicationConflictResolvers)
            {
                RavenJObject metadataToSave;
                RavenJObject documentToSave;
                if (remote.Key != null && local.Key != null && replicationConflictResolver.TryResolveConflict(document.Key, remote.Key.Metadata, remote.Key.DataAsJson, local.Key, key => actions.Documents.DocumentByKey(document.Key),
                    out metadataToSave, out documentToSave))
                {
                    if (metadataToSave != null && metadataToSave.Value<bool>(Constants.RavenDeleteMarker))
                    {

                        database.Documents.Delete(document.Key, null, null);
                    }
                    else
                    {
                        metadataToSave?.Remove(Constants.RavenReplicationConflictDocument);
                        database.Documents.Put(document.Key, document.Etag, documentToSave, metadataToSave, null);
                    }
                    return true;
                }
            }
            return false;
        }

        public static ReplicationConfig GetReplicationConfig(this DocumentDatabase database)
        {
            var configDoc = database.Documents.Get(Constants.RavenReplicationConfig, null);

            if (configDoc == null)
                return null;

            ReplicationConfig config;
            try
            {
                config = configDoc.DataAsJson.JsonDeserialization<ReplicationConfig>();
                return config;
            }
            catch (Exception e)
            {
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

        public static void GetConflictDocuments(this DocumentDatabase database,IEnumerable<JsonDocument> conflicts, out KeyValuePair<JsonDocument, DateTime> local, out KeyValuePair<JsonDocument, DateTime> remote)
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
