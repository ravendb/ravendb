using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Client.Properties;
using Raven.Server.Documents.Expiration;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionCommands;
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
                var state = new JsonParserState();
                var parser = new UnmanagedJsonParser(context, state, "file");

                var commandsProgress = 0L;
                var readers = UnmanagedJsonParserHelper.ReadArrayToMemoryAsync(context, peepingTomStream, parser, state, buffer);
                await using (var readersItr = readers.GetAsyncEnumerator())
                {
                    await ReadStartRecordingDetailsAsync(readersItr, context, peepingTomStream);
                    while (await readersItr.MoveNextAsync())
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

        private class ReplayTxs : IDisposable
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
                var jsonSerializer = GetJsonSerializer();
                StartRecordingDetails startDetail;
                using (var reader = new BlittableJsonReader(context))
                {
                    reader.Initialize(iterator.Current);
                    startDetail = jsonSerializer.Deserialize<StartRecordingDetails>(reader);
                }

                if (string.IsNullOrEmpty(startDetail.Type))
                {
                    throw new ReplayTransactionsException($"Can't read {nameof(RecordingDetails.Type)} of replay detail", peepingTomStream);
                }

                if (string.IsNullOrEmpty(startDetail.Type))
                {
                    throw new ReplayTransactionsException($"Can't read {nameof(StartRecordingDetails.Version)} of replay instructions", peepingTomStream);
                }

                if (startDetail.Version != RavenVersionAttribute.Instance.Build)
                {
                    throw new ReplayTransactionsException($"Can't replay transaction instructions of different server version - Current version({ServerVersion.FullVersion}), Record version({startDetail.Version})", peepingTomStream);
                }
            }
        }

        private static TransactionOperationsMerger.MergedTransactionCommand DeserializeCommand(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string type,
            BlittableJsonReaderObject wrapCmdReader,
            PeepingTomStream peepingTomStream)
        {
            if (!wrapCmdReader.TryGet(nameof(RecordingCommandDetails.Command), out BlittableJsonReaderObject commandReader))
            {
                throw new ReplayTransactionsException($"Can't read {type} for replay", peepingTomStream);
            }

            var jsonSerializer = GetJsonSerializer();
            using (var reader = new BlittableJsonReader(context))
            {
                reader.Initialize(commandReader);
                var dto = DeserializeCommandDto(type, jsonSerializer, reader, peepingTomStream);
                return dto.ToCommand(context, database);
            }
        }

        private static TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> DeserializeCommandDto(
            string type,
            JsonSerializer jsonSerializer,
            BlittableJsonReader reader,
            PeepingTomStream peepingTomStream)
        {
            switch (type)
            {
                case nameof(BatchHandler.MergedBatchCommand):
                    return jsonSerializer.Deserialize<MergedBatchCommandDto>(reader);

                case nameof(DeleteDocumentCommand):
                    return jsonSerializer.Deserialize<DeleteDocumentCommandDto>(reader);

                case nameof(PatchDocumentCommand):
                    return jsonSerializer.Deserialize<PatchDocumentCommandDto>(reader);

                case nameof(DatabaseDestination.MergedBatchPutCommand):
                    return jsonSerializer.Deserialize<DatabaseDestination.MergedBatchPutCommandDto>(reader);

                case nameof(MergedPutCommand):
                    return jsonSerializer.Deserialize<MergedPutCommand.MergedPutCommandDto>(reader);

                case nameof(BulkInsertHandler.MergedInsertBulkCommand):
                    return jsonSerializer.Deserialize<MergedInsertBulkCommandDto>(reader);

                case nameof(AttachmentHandler.MergedPutAttachmentCommand):
                    return jsonSerializer.Deserialize<MergedPutAttachmentCommandDto>(reader);

                case nameof(AttachmentHandler.MergedDeleteAttachmentCommand):
                    return jsonSerializer.Deserialize<MergedDeleteAttachmentCommandDto>(reader);

                case nameof(ResolveConflictOnReplicationConfigurationChange.PutResolvedConflictsCommand):
                    return jsonSerializer.Deserialize<PutResolvedConflictsCommandDto>(reader);

                case nameof(HiLoHandler.MergedNextHiLoCommand):
                    return jsonSerializer.Deserialize<MergedNextHiLoCommandDto>(reader);

                case nameof(HiLoHandler.MergedHiLoReturnCommand):
                    return jsonSerializer.Deserialize<MergedHiLoReturnCommandDto>(reader);

                case nameof(IncomingReplicationHandler.MergedDocumentReplicationCommand):
                    return jsonSerializer.Deserialize<MergedDocumentReplicationCommandDto>(reader);

                case nameof(ExpiredDocumentsCleaner.DeleteExpiredDocumentsCommand):
                    return jsonSerializer.Deserialize<DeleteExpiredDocumentsCommandDto>(reader);

                case nameof(OutgoingReplicationHandler.UpdateSiblingCurrentEtag):
                    return jsonSerializer.Deserialize<UpdateSiblingCurrentEtagDto>(reader);

                case nameof(IncomingReplicationHandler.MergedUpdateDatabaseChangeVectorCommand):
                    return jsonSerializer.Deserialize<MergedUpdateDatabaseChangeVectorCommandDto>(reader);

                case nameof(AdminRevisionsHandler.DeleteRevisionsCommand):
                    return jsonSerializer.Deserialize<DeleteRevisionsCommandDto>(reader);

                case nameof(RevisionsStorage.RevertDocumentsCommand):
                    return jsonSerializer.Deserialize<RevisionsStorage.RevertDocumentsCommandDto>(reader);

                case nameof(RevisionsOperations.DeleteRevisionsBeforeCommand):
                    throw new ReplayTransactionsException(
                        "Because this command is deleting according to revisions' date & the revisions that created by replaying have different date an in place decision needed to be made",
                        peepingTomStream);
                case nameof(TombstoneCleaner.DeleteTombstonesCommand):
                    return jsonSerializer.Deserialize<DeleteTombstonesCommandDto>(reader);

                case nameof(OutputReduceToCollectionCommand):
                    return jsonSerializer.Deserialize<OutputReduceToCollectionCommandDto>(reader);

                case nameof(BatchHandler.ClusterTransactionMergedCommand):
                    return jsonSerializer.Deserialize<ClusterTransactionMergedCommandDto>(reader);

                case nameof(CountersHandler.ExecuteCounterBatchCommand):
                    return jsonSerializer.Deserialize<ExecuteCounterBatchCommandDto>(reader);

                case nameof(TimeSeriesRollups.AddedNewRollupPoliciesCommand):
                    return jsonSerializer.Deserialize<TimeSeriesRollups.AddedNewRollupPoliciesCommand.AddedNewRollupPoliciesCommandDto>(reader);

                case nameof(TimeSeriesRollups.RollupTimeSeriesCommand):
                    return jsonSerializer.Deserialize<TimeSeriesRollups.RollupTimeSeriesCommand.RollupTimeSeriesCommandDto>(reader);

                case nameof(TimeSeriesRollups.TimeSeriesRetentionCommand):
                    return jsonSerializer.Deserialize<TimeSeriesRollups.TimeSeriesRetentionCommand.TimeSeriesRetentionCommandDto>(reader);

                default:
                    throw new ReplayTransactionsException($"Can't read {type} for replay", peepingTomStream);
            }
        }

        internal static JsonSerializer GetJsonSerializer()
        {
            var jsonSerializer = (NewtonsoftJsonJsonSerializer)DocumentConventions.DefaultForServer.Serialization.CreateSerializer();
            jsonSerializer.Converters.Add(SliceJsonConverter.Instance);
            jsonSerializer.Converters.Add(Raven.Server.Json.Converters.BlittableJsonConverter.Instance);
            jsonSerializer.Converters.Add(LazyStringValueJsonConverter.Instance);
            jsonSerializer.Converters.Add(StreamConverter.Instance);
            jsonSerializer.Converters.Add(BlittableJsonReaderArrayConverter.Instance);
            jsonSerializer.Converters.Add(CounterOperationConverter.Instance);
            return jsonSerializer;
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

    internal class ReplayProgress
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

    internal class StartRecordingDetails : RecordingDetails
    {
        private const string DetailsType = "StartRecording";
        public string Version { get; }

        public StartRecordingDetails()
            : base(DetailsType)
        {
            Version = RavenVersionAttribute.Instance.Build;
        }
    }

    internal class RecordingCommandDetails : RecordingDetails
    {
        public TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> Command;

        public RecordingCommandDetails(string type) : base(type)
        {
        }
    }

    public class ReplayTransactionsException : Exception
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
