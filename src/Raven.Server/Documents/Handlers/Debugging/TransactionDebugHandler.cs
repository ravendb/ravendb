using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Debugging.Processors;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public sealed class TransactionDebugHandler : DatabaseRequestHandler
    {
        internal sealed class TransactionInfo
        {
            public string Path;
            public List<TxInfoResult> Information;
        }

        public const string TotalTimeMSecondsSuffix = "mSecs";
        
        [RavenAction("/databases/*/admin/debug/txinfo", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public async Task TxInfo()
        {
            var results = new List<TransactionInfo>();

            foreach (var env in Database.GetAllStoragesEnvironment())
            {
                var txInfo = new TransactionInfo
                {
                    Path = env.Environment.Options.BasePath.FullPath,
                    Information = env.Environment.ActiveTransactions.AllTransactionsInstances.Select(ToTxInfoResult).ToList()
                };
                
                results.Add(txInfo);
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["tx-info"] = ToJson(results)
                });
            }
        }

        internal static TxInfoResult ToTxInfoResult(LowLevelTransaction lowLevelTransaction)
        {
            return new TxInfoResult
            {
                TransactionId = lowLevelTransaction.Id,
                ThreadId = lowLevelTransaction.CurrentTransactionHolder?.ManagedThreadId,
                ThreadName = lowLevelTransaction.CurrentTransactionHolder?.Name,
                CallerName = lowLevelTransaction.CallerName,
                StartTime = lowLevelTransaction.TxStartTime.GetDefaultRavenFormat(isUtc: true),
                TotalTime = $"{(DateTime.UtcNow - lowLevelTransaction.TxStartTime).TotalMilliseconds} {TotalTimeMSecondsSuffix}",
                FlushInProgressLockTaken = lowLevelTransaction.FlushInProgressLockTaken,
                Flags = lowLevelTransaction.Flags,
                IsCloned = lowLevelTransaction.IsCloned,
                NumberOfModifiedPages = lowLevelTransaction.NumberOfModifiedPages,
                Committed = lowLevelTransaction.Committed,
                TotalAllocatedSize = new Size(lowLevelTransaction.TotalAllocatedInBytes, SizeUnit.Bytes).ToString(),
                DecompressedBufferSize = new Size(lowLevelTransaction.DecompressedBufferBytes, SizeUnit.Bytes).ToString(),
                TotalEncryptionBufferSize = lowLevelTransaction.TotalEncryptionBufferInBytes.ToString(),
                IsDisposed = lowLevelTransaction.IsDisposed,
            };
        }

        [RavenAction("/databases/*/admin/debug/cluster/txinfo", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public async Task ClusterTxInfo()
        {
            using (var processor = new TransactionDebugHandlerProcessorForGetClusterInfo(this))
                await processor.ExecuteAsync();
        }

        internal static DynamicJsonArray ToJson(List<TransactionInfo> txInfos)
        {
            return new DynamicJsonArray(txInfos.Select(ToJson));
        }

        private static DynamicJsonValue ToJson(TransactionInfo txinfo)
        {
            return new DynamicJsonValue
            {
                [nameof(StorageEnvironmentOptions.BasePath)] = txinfo.Path,
                [nameof(TransactionInfo.Information)] = new DynamicJsonArray(txinfo.Information.Select(ToJson))
            };
        }

        private static DynamicJsonValue ToJson(TxInfoResult txInfo)
        {
            return new DynamicJsonValue
            {
                [nameof(TxInfoResult.TransactionId)] = txInfo.TransactionId,
                [nameof(TxInfoResult.ThreadId)] = txInfo.ThreadId,
                [nameof(TxInfoResult.ThreadName)] = txInfo.ThreadName,
                [nameof(TxInfoResult.CallerName)] = txInfo.CallerName,
                [nameof(TxInfoResult.StartTime)] = txInfo.StartTime,
                [nameof(TxInfoResult.TotalTime)] = txInfo.TotalTime,
                [nameof(TxInfoResult.FlushInProgressLockTaken)] = txInfo.FlushInProgressLockTaken,
                [nameof(TxInfoResult.Flags)] = txInfo.Flags,
                [nameof(TxInfoResult.IsCloned)] = txInfo.IsCloned,
                [nameof(TxInfoResult.NumberOfModifiedPages)] = txInfo.NumberOfModifiedPages,
                [nameof(TxInfoResult.Committed)] = txInfo.Committed,
                [nameof(TxInfoResult.TotalAllocatedSize)] = txInfo.TotalAllocatedSize,
                [nameof(TxInfoResult.DecompressedBufferSize)] = txInfo.DecompressedBufferSize,
                [nameof(TxInfoResult.TotalEncryptionBufferSize)] = txInfo.TotalEncryptionBufferSize,
                [nameof(TxInfoResult.IsDisposed)] = txInfo.IsDisposed,
            };
        }
    }

    internal sealed class TxInfoResult
    {
        public long TransactionId;
        public int? ThreadId;
        public string ThreadName;
        public string CallerName;
        public string StartTime;
        public string TotalTime;
        public bool FlushInProgressLockTaken;
        public TransactionFlags Flags;
        public bool IsCloned;
        public long NumberOfModifiedPages;
        public bool Committed;
        public string TotalAllocatedSize;
        public string DecompressedBufferSize;
        public string TotalEncryptionBufferSize;
        public bool IsDisposed;
    }
}
