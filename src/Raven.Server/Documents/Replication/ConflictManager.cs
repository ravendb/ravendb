using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Replication
{
    public class ConflictManager
    {
        private readonly DocumentDatabase _database;

        private readonly Logger _log;
        private readonly ReplicationDocument _replicationDocument;
        private readonly Dictionary<string, ScriptResolver> _scriptConflictResolversCache;

        public ConflictManager(DocumentDatabase database, ReplicationDocument replicationDocument,
            Dictionary<string, ScriptResolver> scriptConflictResolversCache)
        {
            _replicationDocument = replicationDocument;
            _scriptConflictResolversCache = scriptConflictResolversCache;
            _database = database;
            _log = LoggingSource.Instance.GetLogger<ConflictManager>(_database.Name);
        }

        public void HandleConflictForDocument(
            DocumentsOperationContext documentsContext,
            IncomingReplicationHandler.ReplicationDocumentsPositions docPosition,
            BlittableJsonReaderObject doc,
            ChangeVectorEntry[] changeVector,
            ChangeVectorEntry[] otherChangeVector
        )
        {
            if (docPosition.Id.StartsWith("Raven/Hilo/", StringComparison.OrdinalIgnoreCase))
            {
                HandleHiloConflict(documentsContext, docPosition, doc);
                return;
            }
            if (_database.DocumentsStorage.TryResolveIdenticalDocument(
                documentsContext,
                docPosition.Id,
                doc,
                docPosition.LastModifiedTicks,
                changeVector))
                return;

            if (TryResovleConflictByScript(
                documentsContext,
                docPosition,
                changeVector,
                doc))
                return;

            if (TryResolveUsingDefaultResolver(
                documentsContext,
                docPosition,
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
                                docPosition.Collection ??
                                CollectionName.GetCollectionName(doc)
                            ),
                            LastModified = new DateTime(docPosition.LastModifiedTicks),
                            LoweredKey = documentsContext.GetLazyString(docPosition.Id),
                            ChangeVector = changeVector
                        }
                    };
                    conflicts.AddRange(documentsContext.DocumentDatabase.DocumentsStorage.GetConflictsFor(
                        documentsContext, docPosition.Id));
                    var localDocumentTuple =
                        documentsContext.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(documentsContext,
                            docPosition.Id, false);
                    var local = DocumentConflict.From(documentsContext, localDocumentTuple.Item1) ?? DocumentConflict.From(localDocumentTuple.Item2);
                    if (local != null)
                        conflicts.Add(local);

                    _database.DocumentsStorage.ResolveToLatest(documentsContext, conflicts, local != null && local.Doc == null);
                    break;
                default:
                    _database.DocumentsStorage.AddConflict(documentsContext, docPosition, doc, changeVector, docPosition.Collection);
                    break;
            }
        }

        private bool TryResovleConflictByScript(
            DocumentsOperationContext documentsContext,
            IncomingReplicationHandler.ReplicationDocumentsPositions docPosition,
            ChangeVectorEntry[] incomingChangeVector,
            BlittableJsonReaderObject doc)
        {
            var conflictedDocs = new List<DocumentConflict>(documentsContext.DocumentDatabase.DocumentsStorage.GetConflictsFor(documentsContext, docPosition.Id));
            var isTomstone = false;

            if (conflictedDocs.Count == 0)
            {
                var relevantLocalDoc = documentsContext.DocumentDatabase.DocumentsStorage
                    .GetDocumentOrTombstone(
                        documentsContext,
                        docPosition.Id);
                if (relevantLocalDoc.Item1 != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(documentsContext, relevantLocalDoc.Item1));
                }
                else if (relevantLocalDoc.Item2 != null)
                {
                    conflictedDocs.Add(DocumentConflict.From(relevantLocalDoc.Item2));
                    isTomstone = true;
                }
            }

            if (conflictedDocs.Count == 0)
                InvalidConflictWhenThereIsNone(docPosition);

            var collection = CollectionName.GetCollectionName(docPosition.Id, doc);

            conflictedDocs.Add(new DocumentConflict
            {
                LoweredKey = conflictedDocs[0].LoweredKey,
                Key = conflictedDocs[0].Key,
                Collection = documentsContext.GetLazyStringForFieldWithCaching(collection),
                ChangeVector = incomingChangeVector,
                Doc = doc
            });

            ScriptResolver scriptResolver;
            var hasScript = _scriptConflictResolversCache.TryGetValue(collection, out scriptResolver);
            if (!hasScript || scriptResolver == null)
            {
                if (_log.IsInfoEnabled)
                    _log.Info($"Script not found to resolve the {collection} collection");
                return false;
            }

            return _database.DocumentsStorage.TryResolveConflictByScriptInternal(
                documentsContext,
                scriptResolver,
                conflictedDocs,
                documentsContext.GetLazyString(collection),
                isTomstone);
        }

        private bool TryResolveUsingDefaultResolver(
            DocumentsOperationContext context,
            IncomingReplicationHandler.ReplicationDocumentsPositions docPosition,
            ChangeVectorEntry[] incomingChangeVector,
            BlittableJsonReaderObject doc)
        {
            var conflicts = new List<DocumentConflict>(_database.DocumentsStorage.GetConflictsFor(context, docPosition.Id));
            var localDocumentTuple = _database.DocumentsStorage.GetDocumentOrTombstone(context, docPosition.Id, false);
            var localDoc = DocumentConflict.From(context, localDocumentTuple.Item1) ??
                           DocumentConflict.From(localDocumentTuple.Item2);
            if (localDoc != null)
                conflicts.Add(localDoc);
            conflicts.Add(new DocumentConflict
            {
                ChangeVector = incomingChangeVector,
                Collection = context.GetLazyStringForFieldWithCaching(
                    docPosition.Collection ??
                    CollectionName.GetCollectionName(docPosition.Id, doc)),
                Doc = doc,
                LoweredKey = context.GetLazyString(docPosition.Id)
            });

            return _database.DocumentsStorage.TryResolveUsingDefaultResolverInternal(
                context,
                _replicationDocument?.DefaultResolver,
                conflicts,
                localDocumentTuple.Item2 != null);
        }

        private void HandleHiloConflict(DocumentsOperationContext context, IncomingReplicationHandler.ReplicationDocumentsPositions docPosition,
            BlittableJsonReaderObject doc)
        {
            long highestMax;
            if (!doc.TryGet("Max", out highestMax))
                throw new InvalidDataException("Tried to resolve HiLo document conflict but failed. Missing property name'Max'");

            var conflicts = _database.DocumentsStorage.GetConflictsFor(context, docPosition.Id);

            var resolvedHiLoDoc = doc;
            if (conflicts.Count == 0)
            {
                //conflict with another existing document
                var localHiloDoc = _database.DocumentsStorage.Get(context, docPosition.Id);
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
            _database.DocumentsStorage.Put(context, docPosition.Id, null, resolvedHiLoDoc);
        }

        private static void InvalidConflictWhenThereIsNone(IncomingReplicationHandler.ReplicationDocumentsPositions docPosition)
        {
            throw new InvalidDataException(
                $"Conflict detected on {docPosition.Id} but there are no conflicts / docs / tombstones for this document");
        }
    }
}