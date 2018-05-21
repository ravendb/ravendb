using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ClusterTransactionBatchCommand : CommandBase
    {
        public List<ClusterTransactionCommand> TransactionBatch;

        public ClusterTransactionBatchCommand() { }

        public ClusterTransactionBatchCommand(List<ClusterTransactionCommand> batch)
        {
            if (batch == null || batch.Count == 0)
            {
                throw new ArgumentException("Cluster Transaction Batch must contain transactions.");
            }
            TransactionBatch = batch;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(TransactionBatch)] = new DynamicJsonArray(TransactionBatch.Select(t => t.ToJson(context)));
            return djv;
        }
    }
}
