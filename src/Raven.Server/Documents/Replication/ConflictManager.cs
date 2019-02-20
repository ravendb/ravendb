using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers;
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
        private readonly ResolveConflictOnReplicationConfigurationChange _conflictResolver;

        public ConflictManager(DocumentDatabase database, ResolveConflictOnReplicationConfigurationChange conflictResolver)
        {
            _conflictResolver = conflictResolver;
            _database = database;
            _log = LoggingSource.Instance.GetLogger<ConflictManager>(_database.Name);
        }

        public unsafe void HandleConflictForDocument(
            DocumentsOperationContext documentsContext,
            string id,
            string collection,
            long lastModifiedTicks,
            BlittableJsonReaderObject doc,
            string changeVector,
            string conflictedChangeVector,
            DocumentFlags flags)
        {
            if (id.StartsWith(HiLoHandler.RavenHiloIdPrefix, StringComparison.OrdinalIgnoreCase))
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

            var lazyId = documentsContext.GetLazyString(id);
            using (DocumentIdWorker.GetLower(documentsContext.Allocator, lazyId, out var loweredKey))
            {
                var conflictedDoc = new DocumentConflict
                {
                    Doc = doc,
                    Collection = documentsContext.GetLazyStringForFieldWithCaching(
                        collection ??
                        CollectionName.GetCollectionName(doc)
                    ),
                    LastModified = new DateTime(lastModifiedTicks),
                    LowerId = documentsContext.AllocateStringValue(null, loweredKey.Content.Ptr, loweredKey.Content.Length),
                    Id = lazyId,
                    ChangeVector = changeVector,
                    Flags = flags
                };

                if (TryResolveConflictByScript(
                    documentsContext,
                    conflictedDoc))
                    return;

                if (_conflictResolver.ConflictSolver?.ResolveToLatest ?? true)
                {
                    if (conflictedChangeVector == null) //precaution
                        throw new InvalidOperationException(
                            "Detected conflict on replication, but could not figure out conflicted vector. This is not supposed to happen and is likely a bug.");

                    var conflicts = new List<DocumentConflict>
                    {
                        conflictedDoc.Clone()
                    };
                    conflicts.AddRange(_database.DocumentsStorage.ConflictsStorage.GetConflictsFor(documentsContext, id));

                    var localDocumentTuple = _database.DocumentsStorage.GetDocumentOrTombstone(documentsContext, id, false);
                    var local = DocumentConflict.From(documentsContext, localDocumentTuple.Document) ?? DocumentConflict.From(localDocumentTuple.Tombstone);
                    if (local != null)
                        conflicts.Add(local);

                    var resolved = _conflictResolver.ResolveToLatest(conflicts);
                    _conflictResolver.PutResolvedDocument(documentsContext, resolved, conflictedDoc);

                    return;
                }

                _database.DocumentsStorage.ConflictsStorage.AddConflict(documentsContext, id, lastModifiedTicks, doc, changeVector, collection, flags);
            }
        }

        private bool TryResolveConflictByScript(
            DocumentsOperationContext documentsContext,
            DocumentConflict conflict)
        {
            var collection = conflict.Collection;

            var hasScript = _conflictResolver.ScriptConflictResolversCache.TryGetValue(collection, out ScriptResolver scriptResolver);
            if (!hasScript || scriptResolver == null)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Script not found to resolve the {collection} collection");
                return false;
            }

            var conflictedDocs = new List<DocumentConflict>(documentsContext.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(documentsContext, conflict.Id));

            if (conflictedDocs.Count == 0)
            {
                var relevantLocalDoc = documentsContext.DocumentDatabase.DocumentsStorage
                    .GetDocumentOrTombstone(
                        documentsContext,
                        conflict.Id);
                if (relevantLocalDoc.Document != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(documentsContext, relevantLocalDoc.Document));
                }
                else if (relevantLocalDoc.Tombstone != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(relevantLocalDoc.Tombstone));
                }
            }

            if (conflictedDocs.Count == 0)
                InvalidConflictWhenThereIsNone(conflict.Id);

            conflictedDocs.Add(conflict.Clone());

            if (_conflictResolver.TryResolveConflictByScriptInternal(
                documentsContext,
                scriptResolver,
                conflictedDocs,
                documentsContext.GetLazyString(collection), out var resolved))
            {
                _conflictResolver.PutResolvedDocument(documentsContext, resolved, conflict);
                return true;
            }

            return false;
        }

        private void HandleHiloConflict(DocumentsOperationContext context, string id, BlittableJsonReaderObject doc, string changeVector)
        {
            if (!doc.TryGet("Max", out long highestMax))
                throw new InvalidDataException("Tried to resolve HiLo document conflict but failed. Missing property name 'Max'");

            var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id);

            var resolvedHiLoDoc = doc;
            string mergedChangeVector;
            if (conflicts.Count == 0)
            {
                //conflict with another existing document
                var localHiloDoc = _database.DocumentsStorage.Get(context, id);
                if (localHiloDoc.Data.TryGet("Max", out double max) && max > highestMax)
                    resolvedHiLoDoc = localHiloDoc.Data;
                mergedChangeVector = ChangeVectorUtils.MergeVectors(changeVector, localHiloDoc.ChangeVector);
            }
            else
            {
                foreach (var conflict in conflicts)
                {
                    if (conflict.Doc.TryGet("Max", out long tmpMax) && tmpMax > highestMax)
                    {
                        highestMax = tmpMax;
                        resolvedHiLoDoc = conflict.Doc;
                    }
                }
                var merged = ChangeVectorUtils.MergeVectors(conflicts.Select(c => c.ChangeVector).ToList());
                mergedChangeVector = ChangeVectorUtils.MergeVectors(merged, changeVector);
            }
            _database.DocumentsStorage.Put(context, id, null, resolvedHiLoDoc,changeVector: mergedChangeVector, nonPersistentFlags: NonPersistentDocumentFlags.FromResolver);
        }

        private static void InvalidConflictWhenThereIsNone(string id)
        {
            throw new InvalidDataException(
                $"Conflict detected on {id} but there are no conflicts / docs / tombstones for this document");
        }

        public bool TryResolveIdenticalDocument(DocumentsOperationContext context, string id,
            BlittableJsonReaderObject incomingDoc,
            long lastModifiedTicks,
            string incomingChangeVector)
        {
            var existing = _database.DocumentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
            var existingDoc = existing.Document;
            var existingTombstone = existing.Tombstone;

            if (existingDoc != null)
            {
                var compareResult = DocumentCompare.IsEqualTo(existingDoc.Data, incomingDoc, true);
                if (compareResult == DocumentCompareResult.NotEqual)
                    return false;

                // no real conflict here, both documents have identical content so we only merge the change vector without increasing the local etag to prevent ping-pong replication
                var mergedChangeVector = ChangeVectorUtils.MergeVectors(incomingChangeVector, existingDoc.ChangeVector);

                var nonPersistentFlags = NonPersistentDocumentFlags.FromResolver;

                nonPersistentFlags |= compareResult.HasFlag(DocumentCompareResult.AttachmentsNotEqual)
                    ? NonPersistentDocumentFlags.ResolveAttachmentsConflict : NonPersistentDocumentFlags.None;
                
                if (compareResult.HasFlag(DocumentCompareResult.CountersNotEqual))                
                    nonPersistentFlags |= NonPersistentDocumentFlags.ResolveCountersConflict;

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
