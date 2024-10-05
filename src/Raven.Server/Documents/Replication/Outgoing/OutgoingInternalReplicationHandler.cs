using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public sealed class OutgoingInternalReplicationHandler : DatabaseOutgoingReplicationHandler
    {
        private long _lastDestinationEtag;

        public OutgoingInternalReplicationHandler(ReplicationLoader parent, DocumentDatabase database, InternalReplication node,
            TcpConnectionInfo connectionInfo) :
            base(parent, database, node, connectionInfo)
        {
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, RavenLogger logger)
        {
            return new InternalReplicationDocumentSender(stream, this, logger);
        }

        protected override void UpdateDestinationChangeVectorHeartbeat(ReplicationMessageReply replicationBatchReply)
        {
            UpdateSibling(replicationBatchReply);
            base.UpdateDestinationChangeVectorHeartbeat(replicationBatchReply);
        }

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
                var changeVector = DocumentsStorage.GetDatabaseChangeVector(context);

                var status = ChangeVectorUtils.GetConflictStatus(_replicationBatchReply.DatabaseChangeVector,
                    changeVector);

                if (status != ConflictStatus.AlreadyMerged)
                    return false;

                var result = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, _replicationBatchReply.CurrentEtag, changeVector);
                return result.IsValid;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                if (string.IsNullOrEmpty(context.LastDatabaseChangeVector))
                    context.LastDatabaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(context);

                var status = ChangeVectorUtils.GetConflictStatus(_replicationBatchReply.DatabaseChangeVector,
                    context.LastDatabaseChangeVector);

                if (status != ConflictStatus.AlreadyMerged)
                    return 0;

                var result = ChangeVectorUtils.TryUpdateChangeVector(_replicationBatchReply.NodeTag, _dbId, _replicationBatchReply.CurrentEtag,
                    context.LastDatabaseChangeVector);
                if (result.IsValid)
                {
                    context.LastReplicationEtagFrom ??= new Dictionary<string, long>();
                    if (context.LastReplicationEtagFrom.ContainsKey(_replicationBatchReply.DatabaseId) == false)
                    {
                        context.LastReplicationEtagFrom[_replicationBatchReply.DatabaseId] = _replicationBatchReply.CurrentEtag;
                    }

                    context.LastDatabaseChangeVector = context.GetChangeVector(result.ChangeVector);

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
                }

                return result.IsValid ? 1 : 0;
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
