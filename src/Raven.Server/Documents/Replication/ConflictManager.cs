﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.Replication
{
    public sealed class ConflictManager
    {
        private readonly DocumentDatabase _database;

        private readonly RavenLogger _log;
        private readonly ResolveConflictOnReplicationConfigurationChange _conflictResolver;

        public ConflictManager(DocumentDatabase database, ResolveConflictOnReplicationConfigurationChange conflictResolver)
        {
            _conflictResolver = conflictResolver;
            _database = database;
            _log = RavenLogManager.Instance.GetLoggerForDatabase<ConflictManager>(_database.Name);
        }

        public unsafe void HandleConflictForDocument(
            DocumentsOperationContext documentsContext,
            string id,
            string collection,
            long lastModifiedTicks,
            BlittableJsonReaderObject doc,
            ChangeVector changeVector,
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

                if (IsSameCollection(documentsContext, id, conflictedDoc.Collection))
                {
                    if (TryResolveConflictByScript(
                        documentsContext,
                        conflictedDoc))
                        return;

                    if (_database.ReplicationLoader.ConflictSolverConfig?.ResolveToLatest ?? true)
                    {
                        var conflicts = new List<DocumentConflict>
                        {
                            conflictedDoc.Clone()
                        };
                        conflicts.AddRange(_database.DocumentsStorage.ConflictsStorage.GetConflictsFor(documentsContext, id));

                        var localDocumentTuple = _database.DocumentsStorage.GetDocumentOrTombstone(documentsContext, id, throwOnConflict: false);
                        var local = DocumentConflict.From(documentsContext, localDocumentTuple.Document) ?? DocumentConflict.From(localDocumentTuple.Tombstone);
                        if (local != null)
                            conflicts.Add(local);

                        var resolved = _conflictResolver.ResolveToLatest(documentsContext, conflicts);

                        _conflictResolver.PutResolvedDocument(documentsContext, resolved, resolvedToLatest: true, conflictedDoc);
                        return;
                    }
                }

                _database.DocumentsStorage.ConflictsStorage.AddConflict(documentsContext, id, lastModifiedTicks, doc, changeVector, collection, flags);
            }
        }

        private bool IsSameCollection(DocumentsOperationContext documentsContext, string id, string collection)
        {
            var existing = _database.DocumentsStorage.GetDocumentOrTombstone(documentsContext, id, throwOnConflict: false);

            if (existing.Document != null)
            {
                if (existing.Document.TryGetMetadata(out BlittableJsonReaderObject metadata) == false)
                    return true;
                metadata.TryGetMember(Client.Constants.Documents.Metadata.Collection, out object res);
                return res == null ?
                    string.Equals(collection, Client.Constants.Documents.Collections.EmptyCollection, StringComparison.OrdinalIgnoreCase) :
                    string.Equals(collection, res.ToString(), StringComparison.OrdinalIgnoreCase);
            }
            if (existing.Tombstone != null)
            {
                if (string.Equals(existing.Tombstone.Collection, collection, StringComparison.OrdinalIgnoreCase))
                    return true;
                if (existing.Tombstone.Collection == null)
                    return string.Equals(collection, Client.Constants.Documents.Collections.EmptyCollection, StringComparison.OrdinalIgnoreCase);
            }
            else
            {
                var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(documentsContext, id);
                //compare to the original document
                return string.Equals(conflicts.First().Collection, collection, StringComparison.OrdinalIgnoreCase);
            }
            return false;
        }

        public static void AssertChangeVectorNotNull(string conflictedChangeVector)
        {
            if (conflictedChangeVector == null) //precaution
                throw new InvalidOperationException(
                    "Detected conflict on replication, but could not figure out conflicted vector. This is not supposed to happen and is likely a bug.");
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
            conflictedDocs.Sort((x, y) => Compare(x, y, documentsContext));

            if (_conflictResolver.TryResolveConflictByScriptInternal(
                documentsContext,
                scriptResolver,
                conflictedDocs,
                documentsContext.GetLazyString(collection), out var resolved))
            {
                _conflictResolver.PutResolvedDocument(documentsContext, resolved, resolvedToLatest: false, incoming: conflict);
                return true;
            }

            return false;
        }

        private void HandleHiloConflict(DocumentsOperationContext context, string id, BlittableJsonReaderObject doc, ChangeVector changeVector)
        {
            long highestMax;
            if (doc == null)
            {
                highestMax = 0;
            }
            else
            {
                if (!doc.TryGet("Max", out highestMax))
                    throw new InvalidDataException("Tried to resolve HiLo document conflict but failed. Missing property name 'Max'");
            }

            var conflicts = _database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id);

            var resolvedHiLoDoc = doc;
            string mergedChangeVector;
            if (conflicts.Count == 0)
            {
                //conflict with another existing document
                var localHiloDoc = _database.DocumentsStorage.Get(context, id);
                if (localHiloDoc.Data.TryGet("Max", out long max) && max > highestMax)
                    resolvedHiLoDoc = localHiloDoc.Data.Clone(context);
                mergedChangeVector = changeVector.MergeWith(localHiloDoc.ChangeVector, context);
            }
            else
            {
                foreach (var conflict in conflicts)
                {
                    if (conflict.Doc.TryGet("Max", out long tmpMax) && tmpMax > highestMax)
                    {
                        highestMax = tmpMax;
                        resolvedHiLoDoc = conflict.Doc.Clone(context);
                    }
                }

                var merged = MergeConflicts(context, conflicts);
                mergedChangeVector = merged.MergeWith(changeVector, context);
            }
            _database.DocumentsStorage.Put(context, id, null, resolvedHiLoDoc, changeVector: mergedChangeVector, nonPersistentFlags: NonPersistentDocumentFlags.FromResolver);
        }

        private static void InvalidConflictWhenThereIsNone(string id)
        {
            throw new InvalidDataException(
                $"Conflict detected on {id} but there are no conflicts / docs / tombstones for this document");
        }

        public bool TryResolveIdenticalDocument(DocumentsOperationContext context, string id,
            BlittableJsonReaderObject incomingDoc,
            long lastModifiedTicks,
            ChangeVector incomingChangeVector)
        {
            var existing = _database.DocumentsStorage.GetDocumentOrTombstone(context, id, throwOnConflict: false);
            var existingDoc = existing.Document;
            var existingTombstone = existing.Tombstone;
            var nonPersistentFlags = NonPersistentDocumentFlags.FromResolver;

            if (existingDoc != null)
            {
                var compareResult = DocumentCompare.IsEqualTo(existingDoc.Data, incomingDoc, DocumentCompare.DocumentCompareOptions.MergeMetadata);
                if (compareResult == DocumentCompareResult.NotEqual)
                    return false;

                // no real conflict here, both documents have identical content so we only merge the change vector without increasing the local etag to prevent ping-pong replication
                var mergedChangeVector = incomingChangeVector.MergeWith(existingDoc.ChangeVector, context);

                nonPersistentFlags |= compareResult.HasFlag(DocumentCompareResult.AttachmentsNotEqual)
                    ? NonPersistentDocumentFlags.ResolveAttachmentsConflict : NonPersistentDocumentFlags.None;

                if (compareResult.HasFlag(DocumentCompareResult.CountersNotEqual))
                    nonPersistentFlags |= NonPersistentDocumentFlags.ResolveCountersConflict;

                if (compareResult.HasFlag(DocumentCompareResult.TimeSeriesNotEqual))
                    nonPersistentFlags |= NonPersistentDocumentFlags.ResolveTimeSeriesConflict;

                _database.DocumentsStorage.Put(context, id, null, incomingDoc, lastModifiedTicks, mergedChangeVector,
                    flags: existingDoc.Flags | DocumentFlags.Resolved,
                    nonPersistentFlags: nonPersistentFlags);
                return true;
            }

            if (existingTombstone != null && incomingDoc == null)
            {
                // Conflict between two tombstones resolves to the local tombstone
                var mergedChangeVector = incomingChangeVector.MergeWith(existingTombstone.ChangeVector, context);

                using (Slice.From(context.Allocator, existingTombstone.LowerId, out Slice lowerId))
                {
                    _database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null, lastModifiedTicks, mergedChangeVector,
                        newFlags: existingTombstone.Flags | DocumentFlags.Resolved, nonPersistentFlags: nonPersistentFlags);
                }
                return true;
            }

            return false;
        }

        public static ChangeVector MergeConflicts(DocumentsOperationContext context, IEnumerable<DocumentConflict> conflicts)
        {
            var merged = context.GetEmptyChangeVector();
            foreach (var doc in conflicts)
            {
                merged = merged.MergeWith(doc.ChangeVector, context);
            }

            return merged;
        }

        public static int Compare(DocumentConflict x, DocumentConflict y, IChangeVectorOperationContext context)
        {
            var cvx = new ChangeVector(x.ChangeVector, context);
            var cvy = new ChangeVector(y.ChangeVector, context);
            return ChangeVector.CompareVersion(cvx, cvy);
        }
    }
}
