using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Server.Documents.DataArchival;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Refresh
{
    public sealed class RefreshStorage(DocumentDatabase database, Transaction tx) : AbstractBackgroundWorkStorage(tx, database, LoggingSource.Instance.GetLogger<DataArchivalStorage>(database.Name), DocumentsByRefresh, Constants.Documents.Metadata.Refresh)
    {
        private const string DocumentsByRefresh = "DocumentsByRefresh";
        protected override void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
        {
            using (var doc = Database.DocumentsStorage.Get(context, lowerId, throwOnConflict: false))
            {
                if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                {
                    throw new InvalidOperationException($"Failed to fetch the metadata of document '{id}'");
                }

                if (HasPassed(metadata, currentTime) == false)
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
            }
        }

        protected override void HandleDocumentConflict(BackgroundWorkParameters options, Slice clonedId, ref List<(Slice LowerId, string Id)> expiredDocs)
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
                        HasPassed(metadata, currentTime))
                        continue;

                    allExpired = false;
                    break;
                }
            }

            return (allExpired, id);
        }
    }
}


