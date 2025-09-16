using System.Collections.Generic;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Revisions
{
    public partial class RevisionsStorage
    {
        public Debugging ForDebug => new Debugging(this);

        public class Debugging
        {
            private readonly RevisionsStorage _storage;
            public Debugging(RevisionsStorage storage)
            {
                _storage = storage;
            }
            // Used to delete corrupted document from the JS admin console
            public bool DeleteRevisionByEtag(long etag)
            {
                using (_storage._documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var index = _storage.RevisionsSchema.FixedSizeIndexes[Schemas.Revisions.AllRevisionsEtagsSlice];
                    var table = new Table(_storage.RevisionsSchema, context.Transaction.InnerTransaction);
                    if (table.FindByIndex(index, etag, out var tvr) == false)
                        return false;
                        
                    using var doc = TableValueToRevision(context, ref tvr, DocumentFields.Data | DocumentFields.ChangeVector | DocumentFields.LowerId);
                    if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false
                        || metadata.TryGet(Constants.Documents.Metadata.Collection, out string collection) == false)
                        return false;
                    
                    var collectionName = new CollectionName(collection);
                    var collectionTable = _storage.EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

                    using (_storage.GetKeyPrefix(context, doc.LowerId, out Slice prefixSlice))
                    {
                        _storage.DeleteRevisionFromTable(context, collectionTable, new Dictionary<string, Table>(), doc, collectionName, context.GetChangeVector(doc.ChangeVector), _storage._database.Time.GetUtcNow().Ticks, doc.Flags);
                        IncrementCountOfRevisions(context, prefixSlice, -1);
                    }
                    
                    tx.Commit();
                    return true;
                }
            }
        }
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
                using (DocumentIdWorker.GetLoweredIdSliceFromId(context, id, out var lowerId))
                using (_parent.GetKeyPrefix(context, lowerId, out var lowerIdPrefix))
                using (GetKeyWithEtag(context, lowerId, etag: long.MaxValue, out var compoundPrefix))
                {
                    var table = _parent.EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
                    var holder = table.SeekOneBackwardFrom(_parent.RevisionsSchema.Indexes[Schemas.Revisions.IdAndEtagSlice], lowerIdPrefix, compoundPrefix);
                    var lastRevision = TableValueToRevision(context, ref holder.Reader, DocumentFields.ChangeVector | DocumentFields.LowerId);
                    _parent.DeleteRevisionFromTable(context, table, new Dictionary<string, Table>(), lastRevision, collectionName, context.GetChangeVector(lastRevision.ChangeVector), _parent._database.Time.GetUtcNow().Ticks, lastRevision.Flags);
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
