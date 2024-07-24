using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Refresh
{
    public sealed class RefreshStorage : AbstractBackgroundWorkStorage
    {
        private const string DocumentsByRefresh = "DocumentsByRefresh";

        public RefreshStorage(DocumentDatabase database, Transaction tx) 
            : base(tx, database, LoggingSource.Instance.GetLogger<RefreshStorage>(database.Name),DocumentsByRefresh, Constants.Documents.Metadata.Refresh)
        {
        }

        protected override void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
        {
            using (var doc = Database.DocumentsStorage.Get(context, lowerId, throwOnConflict: false))
            {
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                    return;

                if (HasPassed(metadata, currentTime, MetadataPropertyName) == false)
                    return;

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
                context.Transaction.ForgetAbout(doc); 
            }
        }

        protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice ticksAsSlice, Slice clonedId, Queue<DocumentExpirationInfo> expiredDocs, ref int totalCount)
        {
            if (ShouldHandleWorkOnCurrentNode(options.DatabaseRecord.Topology, options.NodeTag) == false)
                return;

            (bool allExpired, string id) = GetConflictedExpiration(options.Context, options.CurrentTime, clonedId);

            if (allExpired)
            {
                expiredDocs.Enqueue(new DocumentExpirationInfo(ticksAsSlice, clonedId, id));
                totalCount++;
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
                        HasPassed(metadata, currentTime, MetadataPropertyName))
                        continue;

                    allExpired = false;
                    break;
                }
            }

            return (allExpired, id);
        }
    }
}


