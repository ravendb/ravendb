using System;
using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using Voron;
using Elastic.Clients.Elasticsearch;

namespace Raven.Server.Documents.Revisions;
public partial class RevisionsStorage
{
    internal sealed class DeleteRevisionsByChangeVectorMergedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly string _id;

        private readonly List<string> _cvs;

        private readonly bool _includeForceCreated;

        public long? Result { get; private set; } // number of deleted revisions

        public DeleteRevisionsByChangeVectorMergedCommand(string id, List<string> cvs, bool includeForceCreated)
        {
            _id = id;
            _cvs = cvs;
            _includeForceCreated = includeForceCreated;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = DeleteRevisions(context);
            return 1;
        }

        private long DeleteRevisions(DocumentsOperationContext context)
        {
            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;

            var deleted = 0L;

            var lastModifiedTicks = context.DocumentDatabase.Time.GetUtcNow().Ticks;

            var table = new Table(revisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);

            var writeTables = new Dictionary<string, Table>();

            foreach (var cv in _cvs)
            {
                if (string.IsNullOrEmpty(cv))
                    throw new ArgumentException($"Change Vector is null or empty (document id: '{_id}')");

                Document revision;
                using (Slice.From(context.Allocator, cv, out var cvSlice))
                {
                    if (table.ReadByKey(cvSlice, out TableValueReader tvr) == false)
                    {
                        continue;
                    }

                    revision = TableValueToRevision(context, ref tvr, DocumentFields.ChangeVector | DocumentFields.LowerId | DocumentFields.Id);
                }

                if (revision.Id != _id)
                    throw new InvalidOperationException($"Revision with the cv \"{cv}\" doesn't belong to the doc \"{_id}\" but to the doc \"{revision.Id}\"");

                if (SkipForceCreated(revision))
                    continue;

                using (DocumentIdWorker.GetSliceFromId(context, revision.LowerId, out var lowerId))
                using (revisionsStorage.GetKeyPrefix(context, lowerId, out var lowerIdPrefix))
                {
                    var collectionName = revisionsStorage.GetCollectionFor(context, lowerIdPrefix);
                    if (collectionName == null)
                    {
                        if (revisionsStorage._logger.IsInfoEnabled)
                            revisionsStorage._logger.Info($"Tried to delete revision {revision.ChangeVector} ({revision.LowerId}) but no collection found.");
                        continue;
                    }

                    var collectionTable = revisionsStorage.EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

                    revisionsStorage.DeleteRevisionFromTable(context, collectionTable, writeTables, revision, collectionName, context.GetChangeVector(cv), lastModifiedTicks, revision.Flags);
                    RevisionsStorage.IncrementCountOfRevisions(context, lowerIdPrefix, -1);
                    deleted++;
                }
            }

            return deleted;
        }

        private bool SkipForceCreated(Document revision)
        {
            return _includeForceCreated == false && revision.Flags.Contain(DocumentFlags.ForceCreated);
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new DeleteRevisionsByChangeVectorMergedCommandDto(_id, _cvs, _includeForceCreated);
        }

        public sealed class DeleteRevisionsByChangeVectorMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DeleteRevisionsByChangeVectorMergedCommand>
        {
            public readonly string Id;

            public readonly List<string> Cvs;

            public readonly bool IncludeForceCreated;

            public DeleteRevisionsByChangeVectorMergedCommandDto(string id, List<string> cvs, bool includeForceCreated)
            {
                Id = id;
                Cvs = cvs;
                IncludeForceCreated = includeForceCreated;
            }

            public DeleteRevisionsByChangeVectorMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new DeleteRevisionsByChangeVectorMergedCommand(Id, Cvs, IncludeForceCreated);
            }
        }
    }

}
