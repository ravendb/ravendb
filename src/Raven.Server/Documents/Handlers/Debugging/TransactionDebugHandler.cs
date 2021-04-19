using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public class TransactionDebugHandler : DatabaseRequestHandler
    {
        public class TransactionInfo
        {
            public string Path;
            public List<LowLevelTransaction> Information;
        }

        [RavenAction("/databases/*/admin/debug/txinfo", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public Task TxInfo()
        {
            var results = new List<TransactionInfo>();

            foreach (var env in Database.GetAllStoragesEnvironment())
            {
                var txInfo = new TransactionInfo
                {
                    Path = env.Environment.Options.BasePath.FullPath,
                    Information = env.Environment.ActiveTransactions.AllTransactionsInstances
                };
                results.Add(txInfo);
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["tx-info"] = ToJson(results)
                });
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/debug/cluster/txinfo", "GET", AuthorizationStatus.DatabaseAdmin, IsDebugInformationEndpoint = true)]
        public Task ClusterTxInfo()
        {
            var from = GetLongQueryString("from", false);
            var take = GetIntValueQueryString("take", false) ?? int.MaxValue;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["Results"] = new DynamicJsonArray(ClusterTransactionCommand.ReadCommandsBatch(context, Database.Name, from, take))
                });
            }

            return Task.CompletedTask;
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

        private static DynamicJsonValue ToJson(LowLevelTransaction lowLevelTransaction)
        {
            return new DynamicJsonValue
            {
                [nameof(TxInfoResult.TransactionId)] = lowLevelTransaction.Id,
                [nameof(TxInfoResult.ThreadId)] = lowLevelTransaction.CurrentTransactionHolder?.ManagedThreadId,
                [nameof(TxInfoResult.ThreadName)] = lowLevelTransaction.CurrentTransactionHolder?.Name,
                [nameof(TxInfoResult.StartTime)] = lowLevelTransaction.TxStartTime.GetDefaultRavenFormat(isUtc: true),
                [nameof(TxInfoResult.TotalTime)] = $"{(DateTime.UtcNow - lowLevelTransaction.TxStartTime).TotalMilliseconds} mSecs",
                [nameof(TxInfoResult.FlushInProgressLockTaken)] = lowLevelTransaction.FlushInProgressLockTaken,
                [nameof(TxInfoResult.Flags)] = lowLevelTransaction.Flags,
                [nameof(TxInfoResult.IsCloned)] = lowLevelTransaction.IsCloned,
                [nameof(TxInfoResult.IsLazyTransaction)] = lowLevelTransaction.IsLazyTransaction,
                [nameof(TxInfoResult.NumberOfModifiedPages)] = lowLevelTransaction.NumberOfModifiedPages,
                [nameof(TxInfoResult.Committed)] = lowLevelTransaction.Committed,
                [nameof(TxInfoResult.TotalEncryptionBufferSize)] = lowLevelTransaction.TotalEncryptionBufferSize.ToString(),
            };
        }
    }

    internal class TxInfoResult
    {
        public int TransactionId;
        public int ThreadId;
        public string ThreadName;
        public int StartTime;
        public int TotalTime;
        public bool FlushInProgressLockTaken;
        public TransactionFlags Flags;
        public bool IsCloned;
        public bool IsLazyTransaction;
        public long NumberOfModifiedPages;
        public bool Committed;
        public string TotalEncryptionBufferSize;
    }
}
