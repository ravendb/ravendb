using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public sealed class OutgoingInternalReplicationHandler : DatabaseOutgoingReplicationHandler
    {
        private long _lastDestinationEtag;
        internal string LastAcceptedFullChangeVector { get; private set; }

        public OutgoingInternalReplicationHandler(ReplicationLoader parent, DocumentDatabase database, InternalReplication node,
            TcpConnectionInfo connectionInfo) :
            base(parent, database, node, connectionInfo)
        {
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, Logger logger)
        {
            return new InternalReplicationDocumentSender(stream, this, logger);
        }

        protected override void ProcessHandshakeResponse((ReplicationMessageReply.ReplyType ReplyType, ReplicationMessageReply Reply) response)
        {
            base.ProcessHandshakeResponse(response);

            if (response.ReplyType == ReplicationMessageReply.ReplyType.Ok)
                LastAcceptedFullChangeVector = response.Reply.FullDatabaseChangeVector ?? response.Reply.DatabaseChangeVector;
        }

        protected override void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            UpdateSibling(replicationBatchReply);
            base.UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
            LastAcceptedFullChangeVector = replicationBatchReply.FullDatabaseChangeVector ?? replicationBatchReply.DatabaseChangeVector;
        }

        protected override string GetDestinationChangeVectorForPendingWork() => LastAcceptedFullChangeVector ?? LastAcceptedChangeVector;

        protected override string GetCurrentChangeVectorForPendingWork(DocumentsOperationContext context) => DocumentsStorage.GetFullDatabaseChangeVector(context);

        internal override string GetDestinationChangeVectorFor(ReplicationBatchItem item) =>
            ShouldUseFullDestinationChangeVector(item)
                ? LastAcceptedFullChangeVector ?? LastAcceptedChangeVector
                : LastAcceptedChangeVector;

        private static bool ShouldUseFullDestinationChangeVector(ReplicationBatchItem item) =>
            item.Type switch
            {
                ReplicationBatchItem.ReplicationItemType.Attachment => true,
                ReplicationBatchItem.ReplicationItemType.AttachmentTombstone => true,
                ReplicationBatchItem.ReplicationItemType.CounterGroup => true,
                ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment => true,
                ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange => true,
                _ => false
            };

        public void UpdateSibling(ReplicationMessageReply replicationBatchReply)
        {
            var update = new UpdateSiblingCurrentEtag(replicationBatchReply, _waitForChanges);
            if (update.InitAndValidate(_lastDestinationEtag))
            {
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    if (update.DryRun(ctx))
                    {
                        // we intentionally not waiting here, there is nothing that depends on the timing on this, since this
                        // is purely advisory. We just want to have the information up to date at some point, and we won't
                        // miss anything much if this isn't there.
                        _database.TxMerger.Enqueue(update).IgnoreUnobservedExceptions();
                    }
                }
            }

            _lastDestinationEtag = replicationBatchReply.CurrentEtag;
        }

        internal sealed class UpdateSiblingCurrentEtag : DocumentMergedTransactionCommand
        {
            private readonly ReplicationMessageReply _replicationBatchReply;
            private readonly AsyncManualResetEvent _trigger;
            private string _dbId;

            public UpdateSiblingCurrentEtag(ReplicationMessageReply replicationBatchReply, AsyncManualResetEvent trigger)
            {
                _replicationBatchReply = replicationBatchReply;
                _trigger = trigger;
            }

            public bool InitAndValidate(long lastReceivedEtag)
            {
                if (false == Init())
                {
                    return false;
                }

                return _replicationBatchReply.CurrentEtag >= lastReceivedEtag;
            }

            internal bool Init()
            {
                if (Guid.TryParse(_replicationBatchReply.DatabaseId, out Guid dbGuid) == false)
                    return false;

                if (_replicationBatchReply.CurrentEtag == 0)
                    return false;

                _dbId = dbGuid.ToBase64Unpadded();

                return true;
            }

            internal bool DryRun(DocumentsOperationContext context)
            {
                var regularCurrentEtag =
                    _replicationBatchReply.MatchingRegularChangeVectorEtag > 0
                        ? _replicationBatchReply.MatchingRegularChangeVectorEtag
                        : _replicationBatchReply.CurrentEtag;

                var currentFullChangeVector = DocumentsStorage.GetFullDatabaseChangeVector(context);
                var destinationFullChangeVector = _replicationBatchReply.FullDatabaseChangeVector ?? _replicationBatchReply.DatabaseChangeVector;

                var fullStatus = ChangeVectorUtils.GetConflictStatus(destinationFullChangeVector, currentFullChangeVector);
                var canAdvanceFull = false;

                if (fullStatus == ConflictStatus.AlreadyMerged)
                {
                    var fullResult = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, _replicationBatchReply.CurrentEtag, context.GetChangeVector(currentFullChangeVector));
                    canAdvanceFull = fullResult.IsValid;
                }

                var currentRegularChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                var destinationRegularChangeVector = _replicationBatchReply.DatabaseChangeVector;
                var regularStatus = ChangeVectorUtils.GetConflictStatus(destinationRegularChangeVector, currentRegularChangeVector);
                var canAdvanceRegular = false;

                if (regularStatus == ConflictStatus.AlreadyMerged)
                {
                    var regularResult = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, regularCurrentEtag, context.GetChangeVector(currentRegularChangeVector));
                    canAdvanceRegular = regularResult.IsValid;
                }

                return canAdvanceRegular || canAdvanceFull;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var regularCurrentEtag =
                    _replicationBatchReply.MatchingRegularChangeVectorEtag > 0
                        ? _replicationBatchReply.MatchingRegularChangeVectorEtag
                        : _replicationBatchReply.CurrentEtag;

                var currentFullChangeVector = DocumentsStorage.GetFullDatabaseChangeVector(context);
                var destinationFullChangeVector = _replicationBatchReply.FullDatabaseChangeVector ?? _replicationBatchReply.DatabaseChangeVector;

                var fullStatus = ChangeVectorUtils.GetConflictStatus(destinationFullChangeVector, currentFullChangeVector);
                var fullResult = default((bool IsValid, string ChangeVector));
                var canAdvanceFull = false;

                if (fullStatus == ConflictStatus.AlreadyMerged)
                {
                    fullResult = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, _replicationBatchReply.CurrentEtag, context.GetChangeVector(currentFullChangeVector));
                    canAdvanceFull = fullResult.IsValid;
                }

                var currentRegularChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);
                var destinationRegularChangeVector = _replicationBatchReply.DatabaseChangeVector;
                var regularStatus = ChangeVectorUtils.GetConflictStatus(destinationRegularChangeVector, currentRegularChangeVector);
                var regularResult = default((bool IsValid, string ChangeVector));
                var canAdvanceRegular = false;

                if (regularStatus == ConflictStatus.AlreadyMerged)
                {
                    regularResult = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, regularCurrentEtag, context.GetChangeVector(currentRegularChangeVector));
                    canAdvanceRegular = regularResult.IsValid;
                }

                if (canAdvanceRegular == false && canAdvanceFull == false)
                    return 0;

                context.LastReplicationEtagFrom ??= new Dictionary<string, long>();
                context.LastReplicationEtagFrom[_replicationBatchReply.DatabaseId] = _replicationBatchReply.CurrentEtag;

                if (canAdvanceRegular)
                {
                    var regularChangeVector = context.GetChangeVector(regularResult.ChangeVector);
                    context.LastDatabaseChangeVector = regularChangeVector;
                    context.DocumentDatabase.DocumentsStorage.SetDatabaseChangeVector(context, regularChangeVector);
                }

                if (canAdvanceFull)
                    context.DocumentDatabase.DocumentsStorage.SetFullDatabaseChangeVector(context, fullResult.ChangeVector);

                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
                {
                    try
                    {
                        _trigger.Set();
                    }
                    catch
                    {
                        //
                    }
                };

                return 1;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                return new UpdateSiblingCurrentEtagDto { ReplicationBatchReply = _replicationBatchReply };
            }
        }

        internal sealed class UpdateSiblingCurrentEtagDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, UpdateSiblingCurrentEtag>
        {
            public ReplicationMessageReply ReplicationBatchReply;

            public UpdateSiblingCurrentEtag ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                var command = new UpdateSiblingCurrentEtag(ReplicationBatchReply, new AsyncManualResetEvent());
                command.Init();
                return command;
            }
        }
    }
}
