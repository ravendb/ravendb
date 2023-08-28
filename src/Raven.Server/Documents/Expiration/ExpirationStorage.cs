using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.DataArchival;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Expiration
{
    public sealed class ExpirationStorage : AbstractBackgroundWorkStorage
    {
        private const string DocumentsByExpiration = "DocumentsByExpiration";
        private const string DocumentsByRefresh = "DocumentsByRefresh";

        public ExpirationStorage(DocumentDatabase database, Transaction tx) : base(database, LoggingSource.Instance.GetLogger<DataArchivalStorage>(database.Name))
        {
            tx.CreateTree(DocumentsByExpiration);
            tx.CreateTree(DocumentsByRefresh);
        }

        public void Put(DocumentsOperationContext context, Slice lowerId, BlittableJsonReaderObject metadata)
        {
            var hasExpirationDate = metadata.TryGet(Constants.Documents.Metadata.Expires, out string expirationDate);
            var hasRefreshDate = metadata.TryGet(Constants.Documents.Metadata.Refresh, out string refreshDate);

            if (hasExpirationDate == false && hasRefreshDate == false)
                return;

            if (hasExpirationDate)
                PutInternal(context, lowerId, expirationDate, DocumentsByExpiration);

            if (hasRefreshDate)
                PutInternal(context, lowerId, refreshDate, DocumentsByRefresh);
        }

        public sealed record ExpiredDocumentsOptions : BackgroundProcessDocumentsOptions
        {
            public ExpiredDocumentsOptions(DocumentsOperationContext context, DateTime currentTime, DatabaseTopology databaseTopology, string nodeTag,
                long amountToTake) : base(context, currentTime, databaseTopology, nodeTag, amountToTake)
            {
            }
        }

        public Dictionary<Slice, List<(Slice LowerId, string Id)>> GetExpiredDocuments(ExpiredDocumentsOptions options, out Stopwatch duration,
            CancellationToken cancellationToken)
        {
            return GetDocuments(options, DocumentsByExpiration, Constants.Documents.Metadata.Expires, out duration, cancellationToken);
        }

        public Dictionary<Slice, List<(Slice LowerId, string Id)>> GetDocumentsToRefresh(ExpiredDocumentsOptions options, out Stopwatch duration,
            CancellationToken cancellationToken)
        {
            return GetDocuments(options, DocumentsByRefresh, Constants.Documents.Metadata.Refresh, out duration, cancellationToken);
        }
        

        public int DeleteDocumentsExpiration(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, string Id)>> expired, DateTime currentTime)
        {
            return ProcessReadyDocuments(context, expired, currentTime, DocumentsByExpiration, DeleteExpireDocument);
        }
        
        public int RefreshDocuments(DocumentsOperationContext context, Dictionary<Slice, List<(Slice LowerId, string Id)>> expired, DateTime currentTime)
        {
            return ProcessReadyDocuments(context, expired, currentTime, DocumentsByRefresh, RefreshDocument);
        }

        private bool DeleteExpireDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
        {
            try
            {
                using (var doc = Database.DocumentsStorage.Get(context, lowerId, DocumentFields.Data, throwOnConflict: true))
                {
                    if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                    {
                        throw new InvalidOperationException($"Failed to fetch the metadata of document '{id}'");
                    }
                    
                    if (HasPassed(metadata, currentTime, Constants.Documents.Metadata.Expires) == false) 
                        return false;
                    
                    Database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null);
                }
            }
            catch (DocumentConflictException)
            {
                if (GetConflictedExpiration(context, currentTime, lowerId).AllExpired)
                    Database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null);
            }

            return true;
        }

        private bool RefreshDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
        {
            using (var doc = Database.DocumentsStorage.Get(context, lowerId, throwOnConflict: false))
            {
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                {
                    throw new InvalidOperationException($"Failed to fetch the metadata of document '{id}'");
                }

                if (HasPassed(metadata, currentTime, Constants.Documents.Metadata.Refresh) == false)
                    return false;

                // remove the @refresh tag
                metadata.Modifications = new Sparrow.Json.Parsing.DynamicJsonValue(metadata);
                metadata.Modifications.Remove(Constants.Documents.Metadata.Refresh);

                using (var updated = context.ReadObject(doc.Data, doc.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    try
                    {
                        Database.DocumentsStorage.Put(context, doc.Id, doc.ChangeVector, updated, flags: doc.Flags.Strip(DocumentFlags.FromClusterTransaction));
                    }
                    catch (ConcurrencyException)
                    {
                        // This is expected and safe to ignore
                        // It can happen if there is a mismatch with the Cluster-Transaction-Index, which will
                        // sort itself out when the cluster & database will be in sync again
                    }
                    catch (DocumentConflictException)
                    {
                        // no good way to handle this, we'll wait to resolve
                        // the issue when the conflict is resolved
                    }
                }
            }

            return true;
        }
        
        protected override void HandleDocumentConflict(BackgroundProcessDocumentsOptions options, Slice clonedId, ref List<(Slice LowerId, string Id)> expiredDocs)
        {
            if (ShouldHandleWorkOnCurrentNode(options.DatabaseTopology, options.NodeTag) == false)
                return;

            (bool allExpired, string id) = GetConflictedExpiration(options.Context, options.CurrentTime, clonedId);

            if (allExpired)
            {
                expiredDocs.Add((clonedId, id));
            }
        }

        private (bool AllExpired, string Id) GetConflictedExpiration(DocumentsOperationContext context, DateTime currentTime, Slice clonedId)
        {
            string id = null;
            var allExpired = true;
            var conflicts = Database.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, clonedId);
            
            if (conflicts.Count <= 0)
                return (true, null);
            
            foreach (var conflict in conflicts)
            {
                using (conflict)
                {
                    id = conflict.Id;
                        
                    if (conflict.Doc.TryGetMetadata(out var metadata) &&
                        HasPassed(metadata, currentTime, Constants.Documents.Metadata.Expires))
                        continue;

                    allExpired = false;
                    break;
                }
            }

            return (allExpired, id);
        }
    }
}

