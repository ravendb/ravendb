using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Server.Documents.DataArchival;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Expiration
{
    public sealed class ExpirationStorage(DocumentDatabase database, Transaction tx) : AbstractBackgroundWorkStorage(tx, database, LoggingSource.Instance.GetLogger<DataArchivalStorage>(database.Name), DocumentsByExpiration, Constants.Documents.Metadata.Expires)
    {
        private const string DocumentsByExpiration = "DocumentsByExpiration";

        protected override void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
        {
            try
            {
                using (var doc = Database.DocumentsStorage.Get(context, lowerId, DocumentFields.Data, throwOnConflict: true))
                {
                    if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                    {
                        throw new InvalidOperationException($"Failed to fetch the metadata of document '{id}'");
                    }
                    
                    if (HasPassed(metadata, currentTime, MetadataPropertyName) == false) 
                        return;
                    
                    Database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null);
                }
            }
            catch (DocumentConflictException)
            {
                if (GetConflictedExpiration(context, currentTime, lowerId).AllExpired)
                    Database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null);
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

