using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization;
using Raven.Client.Json.Serialization.SystemTextJson;
using Raven.Client.Json.Serialization.SystemTextJson.Internal;
using Raven.Client.Properties;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Handlers.Processors.BulkInsert;
using Raven.Server.Documents.Handlers.Processors.HiLo;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.QueueSink.Commands;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Json.Converters;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    internal static class ReplayTxCommandHelper
    {
        internal static async IAsyncEnumerable<ReplayProgress> ReplayAsync(DocumentDatabase database, Stream replayStream)
        {
            using (var txs = new ReplayTxs())
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.GetMemoryBuffer(out var buffer))
            await using (var gZipStream = new GZipStream(replayStream, CompressionMode.Decompress, leaveOpen: true))
            {
                var peepingTomStream = new PeepingTomStream(gZipStream, context);
                using var _ = context.AcquireParserState(out var state);
                using var parser = new UnmanagedJsonParser(context, state, "file");

                var commandsProgress = 0L;
                var readers = UnmanagedJsonParserHelper.ReadArrayToMemoryAsync(context, peepingTomStream, parser, state, buffer);
                await using (var readersItr = readers.GetAsyncEnumerator())
                {
                    await ReadStartRecordingDetailsAsync(readersItr, context, peepingTomStream);
                    while (await readersItr.MoveNextAsync().ConfigureAwait(true))
                    {
                        using (readersItr.Current)
                        {
                            if (readersItr.Current.TryGet(nameof(RecordingDetails.Type), out string strType) == false)
                            {
                                throw new ReplayTransactionsException($"Can't read {nameof(RecordingDetails.Type)} of replay detail", peepingTomStream);
                            }

                            if (Enum.TryParse<TxInstruction>(strType, true, out var type))
                            {
                                switch (type)
                                {
                                    case TxInstruction.BeginTx:
                                        database.DocumentsStorage.ContextPool.AllocateOperationContext(out txs.TxCtx);
                                        txs.TxCtx.OpenWriteTransaction();
                                        break;

                                    case TxInstruction.Commit:
                                        txs.TxCtx.Transaction.Commit();
                                        break;

                                    case TxInstruction.DisposeTx:
                                        txs.TxCtx.Dispose();
                                        txs.TxCtx = null;
                                        break;

                                    case TxInstruction.BeginAsyncCommitAndStartNewTransaction:
                                        txs.PrevTx = txs.TxCtx.Transaction;
                                        txs.TxCtx.Transaction = txs.TxCtx.Transaction.BeginAsyncCommitAndStartNewTransaction(txs.TxCtx);
                                        break;

                                    case TxInstruction.EndAsyncCommit:
                                        txs.PrevTx.EndAsyncCommit();
                                        break;

                                    case TxInstruction.DisposePrevTx:
                                        txs.PrevTx.Dispose();
                                        txs.PrevTx = null;
                                        break;
                                }
                                continue;
                            }

                            try
                            {
                                var cmd = DeserializeCommand(context, database, strType, readersItr.Current, peepingTomStream);
                                commandsProgress += cmd.ExecuteDirectly(txs.TxCtx);
                                database.TxMerger.UpdateGlobalReplicationInfoBeforeCommit(txs.TxCtx);
                            }
                            catch (Exception)
                            {
                                //TODO To accept exceptions that was thrown while recording
                                throw;
                            }

                            yield return new ReplayProgress
                            {
                                CommandsProgress = commandsProgress
                            };
                        }
                    }
                }
            }
        }

        private sealed class ReplayTxs : IDisposable
        {
            public DocumentsOperationContext TxCtx;
            public DocumentsTransaction PrevTx;

            public void Dispose()
            {
                TxCtx?.Dispose();
                PrevTx?.Dispose();
            }
        }

        private static async Task ReadStartRecordingDetailsAsync(IAsyncEnumerator<BlittableJsonReaderObject> iterator, DocumentsOperationContext context, PeepingTomStream peepingTomStream)
        {
            if (await iterator.MoveNextAsync().ConfigureAwait(false) == false)
            {
                throw new ReplayTransactionsException("Replay stream is empty", peepingTomStream);
            }
            using (iterator.Current)
            {
                var startDetail = Deserialize<StartRecordingDetails>(context, iterator.Current);

                if (string.IsNullOrEmpty(startDetail.Type))
                {
                    throw new ReplayTransactionsException($"Can't read {nameof(RecordingDetails.Type)} of replay detail", peepingTomStream);
                }

                if (string.IsNullOrEmpty(startDetail.Version))
                {
                    throw new ReplayTransactionsException($"Can't read {nameof(StartRecordingDetails.Version)} of replay instructions", peepingTomStream);
                }

                if (startDetail.Version != RavenVersionAttribute.Instance.Build)
                {
                    throw new ReplayTransactionsException($"Can't replay transaction instructions of different server version - Current version({ServerVersion.FullVersion}), Record version({startDetail.Version})", peepingTomStream);
                }
            }
        }

        private static MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction> DeserializeCommand(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string type,
            BlittableJsonReaderObject wrapCmdReader,
            PeepingTomStream peepingTomStream)
        {
            if (wrapCmdReader.TryGet(nameof(RecordingCommandDetails<DocumentsOperationContext, DocumentsTransaction>.Command), out BlittableJsonReaderObject commandReader) == false)
            {
                throw new ReplayTransactionsException($"Can't read {type} for replay", peepingTomStream);
            }

            var dto = DeserializeCommandDto(type, context, commandReader, peepingTomStream);
            return dto.ToCommand(context, database);
        }

        private static IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> DeserializeCommandDto(
            string type,
            DocumentsOperationContext context,
            BlittableJsonReaderObject reader,
            PeepingTomStream peepingTomStream)
        {
            switch (type)
            {
                case nameof(MergedBatchCommand):
                    return Deserialize<MergedBatchCommandDto>(context, reader);

                case nameof(DeleteDocumentCommand):
                    return Deserialize<DeleteDocumentCommandDto>(context, reader);

                case nameof(PatchDocumentCommand):
                    return Deserialize<PatchDocumentCommandDto>(context, reader);

                case nameof(DatabaseDestination.MergedBatchPutCommand):
                    return Deserialize<DatabaseDestination.MergedBatchPutCommandDto>(context, reader);

                case nameof(MergedPutCommand):
                    return Deserialize<MergedPutCommand.MergedPutCommandDto>(context, reader);

                case nameof(MergedInsertBulkCommand):
                    return Deserialize<MergedInsertBulkCommandDto>(context, reader);

                case nameof(AttachmentHandler.MergedPutAttachmentCommand):
                    return Deserialize<MergedPutAttachmentCommandDto>(context, reader);

                case nameof(AttachmentHandler.MergedDeleteAttachmentCommand):
                    return Deserialize<MergedDeleteAttachmentCommandDto>(context, reader);

                case nameof(ResolveConflictOnReplicationConfigurationChange.PutResolvedConflictsCommand):
                    return Deserialize<PutResolvedConflictsCommandDto>(context, reader);

                case nameof(HiLoHandlerProcessorForGetNextHiLo.MergedNextHiLoCommand):
                    return Deserialize<HiLoHandlerProcessorForGetNextHiLo.MergedNextHiLoCommandDto>(context, reader);

                case nameof(HiLoHandlerProcessorForReturnHiLo.MergedHiLoReturnCommand):
                    return Deserialize<HiLoHandlerProcessorForReturnHiLo.MergedHiLoReturnCommandDto>(context, reader);

                case nameof(IncomingReplicationHandler.MergedDocumentReplicationCommand):
                    return Deserialize<MergedDocumentReplicationCommandDto>(context, reader);

                case nameof(ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand):
                    return Deserialize<DeleteExpiredDocumentsCommandDto>(context, reader);

                case nameof(OutgoingInternalReplicationHandler.UpdateSiblingCurrentEtag):
                    return Deserialize<OutgoingInternalReplicationHandler.UpdateSiblingCurrentEtagDto>(context, reader);

                case nameof(IncomingReplicationHandler.MergedUpdateDatabaseChangeVectorCommand):
                    return Deserialize<IncomingReplicationHandler.MergedUpdateDatabaseChangeVectorCommandDto>(context, reader);

               case nameof(RevisionsStorage.DeleteRevisionsByDocumentIdMergedCommand):
                    return Deserialize<RevisionsStorage.DeleteRevisionsByDocumentIdMergedCommand.DeleteRevisionsByDocumentIdMergedCommandDto>(context, reader);

                case nameof(RevisionsStorage.DeleteRevisionsByChangeVectorMergedCommand):
                    return Deserialize<RevisionsStorage.DeleteRevisionsByChangeVectorMergedCommand.DeleteRevisionsByChangeVectorMergedCommandDto>(context, reader);

                case nameof(RevisionsStorage.RevertDocumentsCommand):
                    return Deserialize<RevisionsStorage.RevertDocumentsCommandDto>(context, reader);

                case nameof(RevisionsOperations.DeleteRevisionsBeforeCommand):
                    throw new ReplayTransactionsException(
                        "Because this command is deleting according to revisions' date & the revisions that created by replaying have different date an in place decision needed to be made",
                        peepingTomStream);
                case nameof(TombstoneCleaner.DeleteTombstonesCommand):
                    return Deserialize<DeleteTombstonesCommandDto>(context, reader);

                case nameof(OutputReduceToCollectionCommand):
                    return Deserialize<OutputReduceToCollectionCommandDto>(context, reader);

                case nameof(ClusterTransactionMergedCommand):
                    return Deserialize<ClusterTransactionMergedCommandDto>(context, reader);

                case nameof(CountersHandler.ExecuteCounterBatchCommand):
                    return Deserialize<ExecuteCounterBatchCommandDto>(context, reader);

                case nameof(TimeSeriesRollups.AddedNewRollupPoliciesCommand):
                    return Deserialize<TimeSeriesRollups.AddedNewRollupPoliciesCommand.AddedNewRollupPoliciesCommandDto>(context, reader);

                case nameof(TimeSeriesRollups.RollupTimeSeriesCommand):
                    return Deserialize<TimeSeriesRollups.RollupTimeSeriesCommand.RollupTimeSeriesCommandDto>(context, reader);

                case nameof(TimeSeriesRollups.TimeSeriesRetentionCommand):
                    return Deserialize<TimeSeriesRollups.TimeSeriesRetentionCommand.TimeSeriesRetentionCommandDto>(context, reader);

                case nameof(BatchQueueSinkScriptCommand):
                    return Deserialize<BatchQueueSinkScriptCommand.Dto>(context, reader);

                default:
                    throw new ReplayTransactionsException($"Can't read {type} for replay", peepingTomStream);
            }
        }

        private static readonly ISerializationConventions RecordingSerialization = CreateRecordingSerialization();

        private static ISerializationConventions CreateRecordingSerialization()
        {
            var conventions = new DocumentConventions
            {
                Serialization = new SystemTextJsonSerializationConventions
                {
                    IgnoreUnsafeMembers = true,
                    UnboundedReadDepth = true,
                    CustomizeJsonSerializerOptions = options =>
                    {
                        DeserializationStackGuard.Register(options);
                        options.Converters.Insert(0, SliceJsonConverter.Instance);
                        options.Converters.Insert(0, Raven.Server.Json.Converters.BlittableJsonConverter.Instance);
                        options.Converters.Insert(0, LazyStringValueJsonConverter.Instance);
                        options.Converters.Insert(0, StreamConverter.Instance);
                        options.Converters.Insert(0, BlittableJsonReaderArrayConverter.Instance);
                        options.Converters.Insert(0, CounterOperationConverter.Instance);
                    }
                }
            };

            conventions.Freeze();
            return conventions.Serialization;
        }

        internal static BlittableJsonReaderObject Serialize(JsonOperationContext context, object value)
        {
            using (RecordingJson.EnterContext(context))
            using (IJsonWriter writer = RecordingSerialization.CreateWriter(context))
            {
                RecordingSerialization.CreateSerializer().Serialize(writer, value);
                writer.FinalizeDocument();

                return writer.CreateReader();
            }
        }

        internal static T Deserialize<T>(JsonOperationContext context, BlittableJsonReaderObject json)
        {
            using (RecordingJson.EnterContext(context))
            using (var reader = new SystemTextJsonBlittableReader())
            {
                reader.Initialize(json);
                return RecordingSerialization.CreateDeserializer().Deserialize<T>(reader);
            }
        }
    }

    internal enum TxInstruction
    {
        BeginTx,
        Commit,
        Rollback,
        DisposeTx,
        BeginAsyncCommitAndStartNewTransaction,
        EndAsyncCommit,
        DisposePrevTx
    }

    internal sealed class ReplayProgress
    {
        public long CommandsProgress;
    }

    internal class RecordingDetails
    {
        public string Type { get; }
        public DateTime DateTime { get; }

        public RecordingDetails(string type)
        {
            Type = type;
            DateTime = DateTime.Now;
        }
    }

    internal sealed class StartRecordingDetails : RecordingDetails
    {
        private const string DetailsType = "StartRecording";
        public string Version { get; }

        public StartRecordingDetails()
            : base(DetailsType)
        {
            Version = RavenVersionAttribute.Instance.Build;
        }
    }

    internal sealed class RecordingCommandDetails<TOperationContext, TTransaction> : RecordingDetails
        where TOperationContext : TransactionOperationContext<TTransaction>
        where TTransaction : RavenTransaction
    {
        public IReplayableCommandDto<TOperationContext, TTransaction, MergedTransactionCommand<TOperationContext, TTransaction>> Command;

        public RecordingCommandDetails(string type) : base(type)
        {
        }
    }

    public sealed class ReplayTransactionsException : Exception
    {
        public string Context { get; }

        public ReplayTransactionsException(string message, PeepingTomStream peepingTomStream)
            : base(message)
        {
            try
            {
                Context = Encodings.Utf8.GetString(peepingTomStream.PeepInReadStream());
            }
            catch (Exception e)
            {
                Context = "Failed to generated peepedWindow: " + e;
            }
        }

        public override string Message => base.Message + ";" + Environment.NewLine + "Context" + Environment.NewLine + Context;
    }
}
