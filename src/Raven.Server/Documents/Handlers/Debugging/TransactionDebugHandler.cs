using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Debugging;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Documents.Handlers.Debugging
{
    class TransactionDebugHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/txinfo", "GET")]
        public Task TxInfo()
        {
            foreach (var env in Database.GetAllStoragesEnvironment())
            {
                JsonOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Name"] = env.Name,
                        ["Information"] = new DynamicJsonArray(env.Environment.ActiveTransactions.AllTransactionsInstances.Select(ToJson))
                    });
                }
                return Task.CompletedTask;
            }

            return Task.CompletedTask;
        }

        private DynamicJsonValue ToJson(LowLevelTransaction lowLevelTransaction)
        {
            return new DynamicJsonValue
            {
                [nameof(TxInfoResult.TransactionId)] = lowLevelTransaction.Id,
                [nameof(TxInfoResult.ThreadId)] = lowLevelTransaction.ThreadId,
                [nameof(TxInfoResult.StartTime)] = lowLevelTransaction.TxStartTime,
                [nameof(TxInfoResult.TotalTime)] = $"{(DateTime.UtcNow - lowLevelTransaction.TxStartTime).Milliseconds} mSecs",
                [nameof(TxInfoResult.FlushInProgressLockTaken)] = lowLevelTransaction.FlushInProgressLockTaken,
                [nameof(TxInfoResult.Flags)] = lowLevelTransaction.Flags,
                [nameof(TxInfoResult.IsLazyTransaction)] = lowLevelTransaction.IsLazyTransaction,
                [nameof(TxInfoResult.NumberOfModifiedPages)] = lowLevelTransaction.NumberOfModifiedPages,
                [nameof(TxInfoResult.Committed)] = lowLevelTransaction.Committed
            };
        }
    }

    internal class TxInfoResult
    {
        public int TransactionId;
        public int ThreadId;
        public int StartTime;
        public int TotalTime;
        public bool FlushInProgressLockTaken;
        public TransactionFlags Flags;
        public bool IsLazyTransaction;
        public long NumberOfModifiedPages;
        public bool Committed;
    }
}