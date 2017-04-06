using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ConflictManager
    {
        private readonly DocumentDatabase _database;

        private readonly Logger _log;
        private readonly ReplicationDocument _replicationDocument;
        private readonly ResolveConflictOnReplicationConfigurationChange _conflictResolver;

        public ConflictManager(DocumentDatabase database, ReplicationDocument replicationDocument, ResolveConflictOnReplicationConfigurationChange conflictResolver)
        {
            _replicationDocument = replicationDocument;
            _conflictResolver = conflictResolver;
            _database = database;
            _log = LoggingSource.Instance.GetLogger<ConflictManager>(_database.Name);
        }

        public void HandleConflictForDocument(
            DocumentsOperationContext documentsContext,
            string id,
            string collection,
            long lastModifiedTicks,
            BlittableJsonReaderObject doc,
            ChangeVectorEntry[] changeVector,
            ChangeVectorEntry[] otherChangeVector
        )
        {
            if (id.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase))
            {
                HandleHiloConflict(documentsContext, id, doc);
                return;
            }
            if (TryResolveIdenticalDocument(
                documentsContext,
                id,
                doc,
                lastModifiedTicks,
                changeVector))
                return;

            if (TryResovleConflictByScript(
                documentsContext,
                id,
                changeVector,
                doc))
                return;

            if (TryResolveUsingDefaultResolver(
                documentsContext,
                id,
                collection,
                changeVector,
                doc))
                return;

            switch (_replicationDocument?.DocumentConflictResolution ?? StraightforwardConflictResolution.None)
            {
                case StraightforwardConflictResolution.ResolveToLatest:
                    if (otherChangeVector == null) //precaution
                        throw new InvalidOperationException(
                            "Detected conflict on replication, but could not figure out conflicted vector. This is not supposed to happen and is likely a bug.");

                    var conflicts = new List<DocumentConflict>
                    {
                        new DocumentConflict
                        {
                            Doc = doc,
                            Collection = documentsContext.GetLazyStringForFieldWithCaching(
                                collection ??
                                CollectionName.GetCollectionName(doc)
                            ),
                            LastModified = new DateTime(lastModifiedTicks),
                            LoweredKey = documentsContext.GetLazyString(id),
                            ChangeVector = changeVector
                        }
                    };
                    conflicts.AddRange(documentsContext.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(
                        documentsContext, id));
                    var localDocumentTuple =
                        documentsContext.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(documentsContext,
                            id, false);
                    var local = DocumentConflict.From(documentsContext, localDocumentTuple.Document) ?? DocumentConflict.From(localDocumentTuple.Tombstone);
                    if (local != null)
                        conflicts.Add(local);

                    _conflictResolver.ResolveToLatest(documentsContext, conflicts);
                    break;
                default:
                    _database.DocumentsStorage.AddConflict(documentsContext, id, lastModifiedTicks, doc, changeVector, collection);
                    break;
            }
        }

        private bool TryResovleConflictByScript(
            DocumentsOperationContext documentsContext,
            string id,
            ChangeVectorEntry[] incomingChangeVector,
            BlittableJsonReaderObject doc)
        {
            var collection = CollectionName.GetCollectionName(id, doc);

            var hasScript = _conflictResolver.ScriptConflictResolversCache.TryGetValue(collection, out ScriptResolver scriptResolver);
            if (!hasScript || scriptResolver == null)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Script not found to resolve the {collection} collection");
                return false;
            }

            var conflictedDocs = new List<DocumentConflict>(documentsContext.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(documentsContext, id));
            var isTomstone = false;

            if (conflictedDocs.Count == 0)
            {
                var relevantLocalDoc = documentsContext.DocumentDatabase.DocumentsStorage
                    .GetDocumentOrTombstone(
                        documentsContext,
                        id);
                if (relevantLocalDoc.Document != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(documentsContext, relevantLocalDoc.Document));
                }
                else if (relevantLocalDoc.Tombstone != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(relevantLocalDoc.Tombstone));
                    isTomstone = true;
                }
            }

            if (conflictedDocs.Count == 0)
                InvalidConflictWhenThereIsNone(id);

            conflictedDocs.Add(new DocumentConflict
            {
                LoweredKey = conflictedDocs[0].LoweredKey,
                Key = conflictedDocs[0].Key,
                Collection = documentsContext.GetLazyStringForFieldWithCaching(collection),
                ChangeVector = incomingChangeVector,
                Doc = doc
            });

            return _conflictResolver.TryResolveConflictByScriptInternal(
                documentsContext,
                scriptResolver,
                conflictedDocs,
                documentsContext.GetLazyString(collection),
                isTomstone);
        }

        private bool TryResolveUsingDefaultResolver(
            DocumentsOperationContext context,
            string id,
            string collection,
            ChangeVectorEntry[] incomingChangeVector,
            BlittableJsonReaderObject doc)
        {
            if (_replicationDocument?.DefaultResolver?.ResolvingDatabaseId == null)
                return false;

            var conflicts = new List<DocumentConflict>(_database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id));
            var localDocumentTuple = _database.DocumentsStorage.GetDocumentOrTombstone(context, id, false);
            var localDoc = DocumentConflict.From(context, localDocumentTuple.Document) ??
                           DocumentConflict.From(localDocumentTuple.Tombstone);
            if (localDoc != null)
                conflicts.Add(localDoc);
            conflicts.Add(new DocumentConflict
            {
                ChangeVector = incomingChangeVector,
                Collection = context.GetLazyStringForFieldWithCaching(
                    collection ??
                    CollectionName.GetCollectionName(id, doc)),
                Doc = doc,
                LoweredKey = context.GetLazyString(id)
            });

            return _conflictResolver.TryResolveUsingDefaultResolverInternal(
                context,
                _replicationDocument?.DefaultResolver,
                conflicts);
        }

        private void HandleHiloConflict(DocumentsOperationContext context, string id, BlittableJsonReaderObject doc)
        {
            long highestMax;
            if (!doc.TryGet("Max", out highestMax))
                throw new InvalidDataException("Tried to resolve HiLo document conflict but failed. Missing property name'Max'");

            var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id);

            var resolvedHiLoDoc = doc;
            if (conflicts.Count == 0)
            {
                //conflict with another existing document
                var localHiloDoc = _database.DocumentsStorage.Get(context, id);
                double max;
                if (localHiloDoc.Data.TryGet("Max", out max) && max > highestMax)
                    resolvedHiLoDoc = localHiloDoc.Data;
            }
            else
            {
                foreach (var conflict in conflicts)
                {
                    long tmpMax;
                    if (conflict.Doc.TryGet("Max", out tmpMax) && tmpMax > highestMax)
                    {
                        highestMax = tmpMax;
                        resolvedHiLoDoc = conflict.Doc;
                    }
                }
            }
            _database.DocumentsStorage.Put(context, id, null, resolvedHiLoDoc);
        }

        private static void InvalidConflictWhenThereIsNone(string id)
        {
            throw new InvalidDataException(
                $"Conflict detected on {id} but there are no conflicts / docs / tombstones for this document");
        }

        public bool TryResolveIdenticalDocument(DocumentsOperationContext context, string key,
            BlittableJsonReaderObject incomingDoc,
            long lastModifiedTicks,
            ChangeVectorEntry[] incomingChangeVector)
        {
            var existing = _database.DocumentsStorage.GetDocumentOrTombstone(context, key, throwOnConflict: false);
            var existingDoc = existing.Document;
            var existingTombstone = existing.Tombstone;

            if (existingDoc != null)
            {
                if (Document.IsEqualTo(existingDoc.Data, incomingDoc, true, _database, context, key) == false)
                    return false;

                var resolveDoc = incomingDoc;
                if (resolveDoc.Modifications != null)
                    // TODO: Improve. No need to ReadObject and build a new metadata, 
                    // since we can just put and it will fill out the attachments from the disk - as it will be different
                    resolveDoc = context.ReadObject(incomingDoc, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                // no real conflict here, both documents have identical content
                var mergedChangeVector = ReplicationUtils.MergeVectors(incomingChangeVector, existingDoc.ChangeVector);
                _database.DocumentsStorage.Put(context, key, null, resolveDoc, lastModifiedTicks, mergedChangeVector);
                return true;
            }

            if (existingTombstone != null && incomingDoc == null)
            {
                // Conflict between two tombstones resolves to the local tombstone
                existingTombstone.ChangeVector = ReplicationUtils.MergeVectors(incomingChangeVector, existingTombstone.ChangeVector);
                Slice loweredKey;
                using (Slice.External(context.Allocator, existingTombstone.LoweredKey, out loweredKey))
                {
                    _database.DocumentsStorage.DeleteConflicts(context, loweredKey, null, existingTombstone.ChangeVector);
                }
                return true;
            }

            return false;
        }
    }
}