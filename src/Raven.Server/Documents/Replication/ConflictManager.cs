using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Server;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public class ConflictManager
    {
        private readonly DocumentDatabase _database;

        private readonly Logger _log;
        private readonly ResolveConflictOnReplicationConfigurationChange _conflictResolver;

        public ConflictManager(DocumentDatabase database, ResolveConflictOnReplicationConfigurationChange conflictResolver)
        {
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
            ChangeVectorEntry[] conflictedChangeVector
        )
        {
            if (id.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase))
            {
                HandleHiloConflict(documentsContext, id, doc, changeVector);
                return;
            }
            if (TryResolveIdenticalDocument(
                documentsContext,
                id,
                doc,
                lastModifiedTicks,
                changeVector))
                return;

            if (TryResolveConflictByScript(
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

            if (_conflictResolver.ConflictSolver.ResolveToLatest)
            {
                if (conflictedChangeVector == null) //precaution
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
                        LowerId = documentsContext.GetLazyString(id),
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

                var resolved = _conflictResolver.ResolveToLatest(documentsContext, conflicts);
                _conflictResolver.PutResolvedDocument(documentsContext, resolved);

                return;
            }
            _database.DocumentsStorage.ConflictsStorage.AddConflict(documentsContext, id, lastModifiedTicks, doc, changeVector, collection);
        }

        private bool TryResolveConflictByScript(
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
            var isTombstone = false;

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
                    isTombstone = true;
                }
            }

            if (conflictedDocs.Count == 0)
                InvalidConflictWhenThereIsNone(id);

            conflictedDocs.Add(new DocumentConflict
            {
                LowerId = conflictedDocs[0].LowerId,
                Id = conflictedDocs[0].Id,
                Collection = documentsContext.GetLazyStringForFieldWithCaching(collection),
                ChangeVector = incomingChangeVector,
                Doc = doc
            });

            if (_conflictResolver.TryResolveConflictByScriptInternal(
                documentsContext,
                scriptResolver,
                conflictedDocs,
                documentsContext.GetLazyString(collection),
                isTombstone, out var resolved))
            {
                _conflictResolver.PutResolvedDocument(documentsContext, resolved);
                return true;
            }

            return false;
        }

        private bool TryResolveUsingDefaultResolver(
            DocumentsOperationContext context,
            string id,
            string collection,
            ChangeVectorEntry[] incomingChangeVector,
            BlittableJsonReaderObject doc)
        {
            if (_conflictResolver.ConflictSolver?.DatabaseResolverId == null)
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
                LowerId = context.GetLazyString(id)
            });

            if (_conflictResolver.TryResolveUsingDefaultResolverInternal(
                context,
                _conflictResolver.ConflictSolver.DatabaseResolverId,
                conflicts, out var resolved))
            {
                _conflictResolver.PutResolvedDocument(context, resolved);
                return true;
            }

            return false;
        }

        private void HandleHiloConflict(DocumentsOperationContext context, string id, BlittableJsonReaderObject doc, ChangeVectorEntry[] changeVector)
        {
            long highestMax;
            if (!doc.TryGet("Max", out highestMax))
                throw new InvalidDataException("Tried to resolve HiLo document conflict but failed. Missing property name'Max'");

            var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id);

            var resolvedHiLoDoc = doc;
            ChangeVectorEntry[] mergedChangeVector;
            if (conflicts.Count == 0)
            {
                //conflict with another existing document
                var localHiloDoc = _database.DocumentsStorage.Get(context, id);
                double max;
                if (localHiloDoc.Data.TryGet("Max", out max) && max > highestMax)
                    resolvedHiLoDoc = localHiloDoc.Data;
                mergedChangeVector = ChangeVectorUtils.MergeVectors(changeVector,localHiloDoc.ChangeVector);
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
                var merged = ChangeVectorUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
                mergedChangeVector = ChangeVectorUtils.MergeVectors(merged, changeVector);
            }
            _database.DocumentsStorage.Put(context, id, null, resolvedHiLoDoc,changeVector: mergedChangeVector);
        }

        private static void InvalidConflictWhenThereIsNone(string id)
        {
            throw new InvalidDataException(
                $"Conflict detected on {id} but there are no conflicts / docs / tombstones for this document");
        }

        public bool TryResolveIdenticalDocument(DocumentsOperationContext context, string id,
            BlittableJsonReaderObject incomingDoc,
            long lastModifiedTicks,
            ChangeVectorEntry[] incomingChangeVector)
        {
            var existing = _database.DocumentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
            var existingDoc = existing.Document;
            var existingTombstone = existing.Tombstone;

            if (existingDoc != null)
            {
                var compareResult = DocumentCompare.IsEqualTo(existingDoc.Data, incomingDoc, true);
                if (compareResult == DocumentCompareResult.NotEqual)
                    return false;

                // no real conflict here, both documents have identical content
                var mergedChangeVector = ChangeVectorUtils.MergeVectors(incomingChangeVector, existingDoc.ChangeVector);
                var nonPersistentFlags = (compareResult & DocumentCompareResult.ShouldRecreateDocument) == DocumentCompareResult.ShouldRecreateDocument 
                    ? NonPersistentDocumentFlags.ResolvedAttachmentConflict : NonPersistentDocumentFlags.None;
                _database.DocumentsStorage.Put(context, id, null, incomingDoc, lastModifiedTicks, mergedChangeVector, nonPersistentFlags: nonPersistentFlags);
                return true;
            }

            if (existingTombstone != null && incomingDoc == null)
            {
                // Conflict between two tombstones resolves to the local tombstone
                existingTombstone.ChangeVector = ChangeVectorUtils.MergeVectors(incomingChangeVector, existingTombstone.ChangeVector);
                using (Slice.External(context.Allocator, existingTombstone.LowerId, out Slice lowerId))
                {
                    _database.DocumentsStorage.ConflictsStorage.DeleteConflicts(context, lowerId, null, existingTombstone.ChangeVector);
                }
                return true;
            }

            return false;
        }
    }
}