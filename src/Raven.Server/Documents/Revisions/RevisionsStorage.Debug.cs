﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Revisions
{
    public partial class RevisionsStorage
    {
        internal class TestingStuff
        {
            private RevisionsStorage _parent;

            public TestingStuff(RevisionsStorage revisionsStorage)
            {
                _parent = revisionsStorage;
            }

            internal void DeleteLastRevisionFor(DocumentsOperationContext context, string id, string collection)
            {
                var collectionName = new CollectionName(collection);
                using (DocumentIdWorker.GetSliceFromId(context, id, out var lowerId))
                using (_parent.GetKeyPrefix(context, lowerId, out var lowerIdPrefix))
                using (GetKeyWithEtag(context, lowerId, etag: long.MaxValue, out var compoundPrefix))
                {
                    var table = _parent.EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                    var holder = table.SeekOneBackwardFrom(RevisionsStorage.RevisionsSchema.Indexes[RevisionsStorage.IdAndEtagSlice], lowerIdPrefix, compoundPrefix);
                    var lastRevision = TableValueToRevision(context, ref holder.Reader, DocumentFields.ChangeVector | DocumentFields.LowerId);
                    _parent.DeleteRevisionFromTable(context, table, new Dictionary<string, Table>(), lastRevision, collectionName, lastRevision.ChangeVector, _parent._database.Time.GetUtcNow().Ticks);
                    IncrementCountOfRevisions(context, lowerIdPrefix, -1);
                }
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff(this);
        }
    }
}