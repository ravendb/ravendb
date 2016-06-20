using System;
using System.Diagnostics;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Bundles.Replication.Responders.Behaviors;
using Raven.Database.Bundles.Replication.Triggers;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Json.Linq;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Bundles.Replication.Responders
{
    public class DocumentReplicationBehavior : SingleItemReplicationBehavior<JsonDocument, RavenJObject>
    {
        public IEnumerable<AbstractDocumentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }

        protected override ReplicationConflictTypes ReplicationConflict
        {
            get { return ReplicationConflictTypes.DocumentReplicationConflict; }
        }

        protected override void DeleteItem(string id, Etag etag)
        {
            Database.Documents.Delete(id, etag, null);
        }

        protected override void MarkAsDeleted(string id, RavenJObject metadata)
        {
            Actions.Lists.Set(Constants.RavenReplicationDocsTombstones, id, metadata, UuidType.Documents);
        }

        protected override void AddWithoutConflict(string id, Etag etag, RavenJObject metadata, RavenJObject incoming)
        {
            Database.Documents.Put(id, etag, incoming, metadata, null);
            Actions.Lists.Remove(Constants.RavenReplicationDocsTombstones, id);
        }

        protected override CreatedConflict CreateConflict(string id, string newDocumentConflictId,
            string existingDocumentConflictId, JsonDocument existingItem, RavenJObject existingMetadata)
        {
            existingMetadata.Add(Constants.RavenReplicationConflictDocument, true);
            existingMetadata.Add(Constants.RavenReplicationConflict, true);
            Actions.Documents.AddDocument(existingDocumentConflictId, Etag.Empty, existingItem.DataAsJson, existingItem.Metadata);
            var etag = existingMetadata.Value<bool>(Constants.RavenDeleteMarker) ? Etag.Empty : existingItem.Etag;
            Actions.Lists.Remove(Constants.RavenReplicationDocsTombstones, id);
            var conflictsArray = new RavenJArray(existingDocumentConflictId, newDocumentConflictId);
            var addResult = Actions.Documents.AddDocument(id, etag,
                                                          new RavenJObject
                                                          {
                                                              {"Conflicts", conflictsArray}
                                                          },
                                                          new RavenJObject
                                                          {
                                                              {Constants.RavenReplicationConflict, true},
                                                              {"@Http-Status-Code", 409},
                                                              {"@Http-Status-Description", "Conflict"}
                                                          });

            return new CreatedConflict()
            {
                Etag = addResult.Etag,
                ConflictedIds = conflictsArray.Select(x => x.Value<string>()).ToArray()
            };
        }

        protected override CreatedConflict AppendToCurrentItemConflicts(string id, string newConflictId, RavenJObject existingMetadata, JsonDocument existingItem)
        {
            // just update the current doc with the new conflict document
            RavenJArray ravenJArray;
            existingItem.DataAsJson["Conflicts"] =
                ravenJArray = new RavenJArray(existingItem.DataAsJson.Value<RavenJArray>("Conflicts"));

            var conflictEtag = existingItem.Etag;
            if (ravenJArray.Contains(newConflictId) == false)
            {
                ravenJArray.Add(newConflictId);
                var addResult = Actions.Documents.AddDocument(id, existingItem.Etag, existingItem.DataAsJson, existingItem.Metadata);
                conflictEtag = addResult.Etag;
            }

            return new CreatedConflict
            {
                Etag = conflictEtag,
                ConflictedIds = ravenJArray.Select(x => x.Value<string>()).ToArray()
            };
        }

        protected override RavenJObject TryGetExisting(string id, out JsonDocument existingItem, out Etag existingEtag, out bool deleted)
        {
            var existingDoc = Actions.Documents.DocumentByKey(id);
            if (existingDoc != null)
            {
                ReplicationTask.EnsureReplicationInformationInMetadata(existingDoc.Metadata, Database);

                existingItem = existingDoc;
                existingEtag = existingDoc.Etag;
                deleted = false;
                return existingDoc.Metadata;
            }

            var listItem = Actions.Lists.Read(Constants.RavenReplicationDocsTombstones, id);
            if (listItem != null)
            {
                ReplicationTask.EnsureReplicationInformationInMetadata(listItem.Data, Database);

                existingEtag = listItem.Etag;
                deleted = true;
                existingItem = new JsonDocument
                {
                    Etag = listItem.Etag,
                    DataAsJson = new RavenJObject(),
                    Key = listItem.Key,
                    Metadata = listItem.Data
                };
                return listItem.Data;
            }
            existingEtag = Etag.Empty;
            existingItem = null;
            deleted = false;
            return null;

        }

        protected override bool TryResolveConflict(string id, RavenJObject metadata, RavenJObject document, JsonDocument existing, out RavenJObject metadataToSave,
                                        out RavenJObject documentToSave)
        {
            foreach (var replicationConflictResolver in ReplicationConflictResolvers)
            {
                if (replicationConflictResolver.TryResolveConflict(id, metadata, document, existing, key => Actions.Documents.DocumentByKey(key),
                                                           out metadataToSave, out documentToSave))
                    return true;
            }

            metadataToSave = null;
            documentToSave = null;

            return false;
        }
    }
}
