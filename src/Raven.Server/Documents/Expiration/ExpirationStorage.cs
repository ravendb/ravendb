using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Expiration
{
    public sealed class ExpirationStorage : AbstractBackgroundWorkStorage
    {
        private const string DocumentsByExpiration = "DocumentsByExpiration";

        public ExpirationStorage(DocumentDatabase database, Transaction tx)
            : base(tx, database, DocumentsByExpiration, Constants.Documents.Metadata.Expires)
        {
        }

        protected override void ProcessDocument(DocumentsOperationContext context, Slice lowerId, string id, DateTime currentTime)
        {
            try
            {
                using (var doc = Database.DocumentsStorage.Get(context, lowerId, DocumentFields.Data, throwOnConflict: true))
                {
                    if (doc == null || doc.TryGetMetadata(out var metadata) == false)
                        return;

                    if (HasPassed(metadata, currentTime, MetadataPropertyName) == false)
                        return;

                    Database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null);
                    context.Transaction.ForgetAbout(doc); 
                }
            }
            catch (DocumentConflictException)
            {
                if (GetConflictedExpiration(context, currentTime, lowerId).AllExpired)
                    Database.DocumentsStorage.Delete(context, lowerId, id, expectedChangeVector: null);
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

