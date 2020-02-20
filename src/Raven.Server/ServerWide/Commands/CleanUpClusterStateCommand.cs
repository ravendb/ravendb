using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class CleanUpClusterStateCommand : CommandBase
    {
        public Dictionary<string, long> ClusterTransactionsCleanup;

        public CleanUpClusterStateCommand()
        {
        }

        public CleanUpClusterStateCommand(string uniqueRequestId) : base(uniqueRequestId)
        {
        }

        public unsafe Dictionary<string, long> Clean(ClusterOperationContext context, long index)
        {
            var affectedDatabases = new Dictionary<string, long>();
            foreach (var tuple in ClusterTransactionsCleanup)
            {
                var database = tuple.Key;
                var upToCommandCount = tuple.Value - 1;

                var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);
                using (ClusterTransactionCommand.GetPrefix(context, database, out var prefixSlice))
                {
                    var deleted = items.DeleteByPrimaryKeyPrefix(prefixSlice, shouldAbort: (tvb) =>
                    {
                        var value = tvb.Reader.Read((int)ClusterTransactionCommand.TransactionCommandsColumn.Key, out var size);
                        var prevCommandsCount = Bits.SwapBytes(*(long*)(value + size - sizeof(long)));
                        return prevCommandsCount > upToCommandCount;
                    });
                   
                    if (deleted)
                    {
                        affectedDatabases.Add(database, tuple.Value);
                    }
                }
            }
            return affectedDatabases;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);

            djv[nameof(ClusterTransactionsCleanup)] = DynamicJsonValue.Convert(ClusterTransactionsCleanup);

            return djv;
        }
    }
}
