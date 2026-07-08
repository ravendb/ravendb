using Raven.Server.Documents.Replication.Stats;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;

namespace Raven.Server.Documents.Replication.Incoming
{
    public partial class IncomingReplicationHandler
    {
        internal abstract class MergedUpdateDatabaseChangeVectorCommandBase : DocumentMergedTransactionCommand
        {
            protected readonly string ChangeVector;
            protected readonly long LastDocumentEtag;
            protected readonly IncomingConnectionInfo ConnectionInfo;
            protected readonly AsyncManualResetEvent Trigger;

            protected MergedUpdateDatabaseChangeVectorCommandBase(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger)
            {
                ChangeVector = changeVector;
                LastDocumentEtag = lastDocumentEtag;
                ConnectionInfo = connectionInfo;
                Trigger = trigger;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var operationsCount = 0;
                var lastReplicatedEtag = DocumentsStorage.GetLastReplicatedEtagFrom(context, ConnectionInfo.SourceDatabaseId);
                if (LastDocumentEtag > lastReplicatedEtag)
                {
                    DocumentsStorage.SetLastReplicatedEtagFrom(context, ConnectionInfo.SourceDatabaseId, LastDocumentEtag);
                    operationsCount++;
                }

                if (TryUpdateChangeVector(context))
                    operationsCount++;

                return operationsCount;
            }

            public abstract override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context);

            protected MergedUpdateDatabaseChangeVectorCommandDto CreateDto()
            {
                return new MergedUpdateDatabaseChangeVectorCommandDto
                {
                    ChangeVector = ChangeVector,
                    LastDocumentEtag = LastDocumentEtag,
                    IncomingConnectionInfo = ConnectionInfo
                };
            }

            protected abstract bool TryUpdateChangeVector(DocumentsOperationContext context);
        }

        internal sealed class MergedUpdateDatabaseChangeVectorCommand : MergedUpdateDatabaseChangeVectorCommandBase
        {
            public MergedUpdateDatabaseChangeVectorCommand(string changeVector, long lastDocumentEtag, IncomingConnectionInfo connectionInfo, AsyncManualResetEvent trigger)
                : base(changeVector, lastDocumentEtag, connectionInfo, trigger)
            {
            }

            protected override bool TryUpdateChangeVector(DocumentsOperationContext context)
            {
                var current = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(ChangeVector, current);
                if (conflictStatus != ConflictStatus.Update)
                {
                    if (string.IsNullOrEmpty(ConnectionInfo.SourceDatabaseBase64Id) == false)
                    {
                        var result = ChangeVectorUtils.TryUpdateChangeVector(ConnectionInfo.SourceTag, ConnectionInfo.SourceDatabaseBase64Id, LastDocumentEtag, current);
                        if (result.IsValid)
                        {
                            context.LastDatabaseChangeVector = context.GetChangeVector(result.ChangeVector);
                        }
                    }

                    return false;
                }

                context.LastDatabaseChangeVector = current.MergeWith(ChangeVector, context);
                context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
                {
                    try
                    {
                        Trigger.Set();
                    }
                    catch
                    {
                        //
                    }
                };

                return true;
            }

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
            {
                return CreateDto();
            }
        }

        internal sealed class MergedUpdateDatabaseChangeVectorCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedUpdateDatabaseChangeVectorCommand>
        {
            public string ChangeVector;
            public long LastDocumentEtag;
            public IncomingConnectionInfo IncomingConnectionInfo;

            public MergedUpdateDatabaseChangeVectorCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new MergedUpdateDatabaseChangeVectorCommand(ChangeVector, LastDocumentEtag, IncomingConnectionInfo, new AsyncManualResetEvent());
            }
        }
    }
}
