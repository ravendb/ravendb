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

        public unsafe Dictionary<string, long> Clean(TransactionOperationContext context, long index)
        {
            var affectedDatabases = new Dictionary<string, long>();
            foreach (var tuple in ClusterTransactionsCleanup)
            {
                var database = tuple.Key;
                var upToIndex = tuple.Value;

                var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.TransactionCommandsSchema, ClusterStateMachine.TransactionCommands);
                using (ClusterTransactionCommand.GetPrefix(context, database, out var prefixSlice))
                {
                    var schemaIndexDef = ClusterStateMachine.TransactionCommandsSchema.Indexes[ClusterStateMachine.CommandByDatabaseAndCount];
                    var deleted = items.DeleteForwardFrom(schemaIndexDef, prefixSlice, 
                        startsWith: true, 
                        numberOfEntriesToDelete: long.MaxValue,
                        shouldAbort: (tvb) =>
                        {
                            var value = *(long*)tvb.Reader.Read((int)ClusterTransactionCommand.TransactionCommandsColumn.PreviousCount, out var _);
                            var currentIndex = Bits.SwapBytes(value);
                            return currentIndex > upToIndex;
                        });

                    if (deleted > 0)
                    {
                        affectedDatabases.Add(database, upToIndex);
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
