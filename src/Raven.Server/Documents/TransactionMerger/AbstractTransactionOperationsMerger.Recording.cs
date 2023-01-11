using Raven.Server.ServerWide.Context;
using System.Threading;
using Sparrow.Json;
using Sparrow.Json.Sync;
using System;
using System.IO;
using System.IO.Compression;
using Raven.Client.Json.Serialization.NewtonsoftJson.Internal;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.TransactionMerger;

public abstract partial class AbstractTransactionOperationsMerger<TOperationContext, TTransaction>
    where TOperationContext : TransactionOperationContext<TTransaction>
    where TTransaction : RavenTransaction
{
    private RecordingTx _recording;

    private struct RecordingTx
    {
        public RecordingState State;
        public Stream Stream;
        public Action StopAction;
    }

    public void StartRecording(string filePath, Action stopAction)
    {
        var recordingFileStream = new FileStream(filePath, FileMode.Create);
        if (null != Interlocked.CompareExchange(ref _recording.State, new RecordingState.BeforeEnabledRecordingState(this), null))
        {
            recordingFileStream.Dispose();
            File.Delete(filePath);
        }
        _recording.Stream = new GZipStream(recordingFileStream, CompressionMode.Compress);
        _recording.StopAction = stopAction;
    }

    public bool RecordingEnabled => _recording.State != null;

    public void StopRecording()
    {
        var recordingState = _recording.State;
        if (recordingState != null)
        {
            recordingState.Shutdown();
            _waitHandle.Set();
            _recording.StopAction?.Invoke();
            _recording.StopAction = null;
        }
    }

    public class EnabledRecordingState : RecordingState
    {
        private readonly AbstractTransactionOperationsMerger<TOperationContext, TTransaction> _txMerger;
        private int _isDisposed = 0;

        public EnabledRecordingState(AbstractTransactionOperationsMerger<TOperationContext, TTransaction> txMerger)
        {
            _txMerger = txMerger;
        }

        public override void TryRecord(TOperationContext context, MergedTransactionCommand<TOperationContext, TTransaction> operation)
        {
            var obj = new RecordingCommandDetails<TOperationContext, TTransaction>(operation.GetType().Name)
            {
                Command = operation.ToDto(context)
            };

            TryRecord(obj, context);
        }

        internal override void TryRecord(TOperationContext ctx, TxInstruction tx, bool doRecord = true)
        {
            if (doRecord == false)
            {
                return;
            }

            var commandDetails = new RecordingDetails(tx.ToString());

            TryRecord(commandDetails, ctx);
        }

        private void TryRecord(RecordingDetails commandDetails, JsonOperationContext context)
        {
            try
            {
                using (var commandDetailsReader = RecordingState.SerializeRecordingCommandDetails(context, commandDetails))
                using (var writer = new BlittableJsonTextWriter(context, _txMerger._recording.Stream))
                {
                    writer.WriteComma();
                    context.Write(writer, commandDetailsReader);
                }
            }
            catch
            {
                // ignored
            }
        }

        public override void Prepare(ref RecordingState state)
        {
            if (IsShutdown == false)
                return;

            state = null;
            Dispose();

            _txMerger._recording.Stream?.Dispose();
            _txMerger._recording.Stream = null;
        }

        public override void Dispose()
        {
            if (1 == Interlocked.CompareExchange(ref _isDisposed, 1, 0))
            {
                return;
            }

            using (_txMerger._contextPool.AllocateOperationContext(out TOperationContext ctx))
            {
                using (var writer = new BlittableJsonTextWriter(ctx, _txMerger._recording.Stream))
                {
                    writer.WriteEndArray();
                }
            }
        }
    }

    public abstract class RecordingState : IDisposable
    {
        public abstract void TryRecord(TOperationContext context, MergedTransactionCommand<TOperationContext, TTransaction> cmd);

        internal abstract void TryRecord(TOperationContext context, TxInstruction tx, bool doRecord = true);

        public abstract void Prepare(ref RecordingState state);

        internal static BlittableJsonReaderObject SerializeRecordingCommandDetails(
            JsonOperationContext context,
            RecordingDetails commandDetails)
        {
            using (var writer = new BlittableJsonWriter(context))
            {
                var jsonSerializer = ReplayTxCommandHelper.GetJsonSerializer();

                jsonSerializer.Serialize(writer, commandDetails);
                writer.FinalizeDocument();

                return writer.CreateReader();
            }
        }


        public class BeforeEnabledRecordingState : RecordingState
        {
            private readonly AbstractTransactionOperationsMerger<TOperationContext, TTransaction> _txMerger;

            public BeforeEnabledRecordingState(AbstractTransactionOperationsMerger<TOperationContext, TTransaction> txMerger)
            {
                _txMerger = txMerger;
            }

            public override void TryRecord(TOperationContext context, MergedTransactionCommand<TOperationContext, TTransaction> cmd)
            {
            }

            internal override void TryRecord(TOperationContext context, TxInstruction tx, bool doRecord = true)
            {
            }

            public override void Prepare(ref RecordingState state)
            {
                if (IsShutdown)
                {
                    state = null;
                    return;
                }

                using (_txMerger._contextPool.AllocateOperationContext(out TOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, _txMerger._recording.Stream))
                {
                    writer.WriteStartArray();

                    var commandDetails = new StartRecordingDetails();
                    var commandDetailsReader = SerializeRecordingCommandDetails(context, commandDetails);

                    context.Write(writer, commandDetailsReader);
                }

                state = new EnabledRecordingState(_txMerger);
            }

            public override void Dispose()
            {
            }
        }

        protected bool IsShutdown { private set; get; }

        public void Shutdown()
        {
            IsShutdown = true;
        }

        public abstract void Dispose();
    }
}

