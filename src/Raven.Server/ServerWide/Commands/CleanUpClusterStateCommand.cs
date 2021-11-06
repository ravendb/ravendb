using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Data.Tables;

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

        public Dictionary<string, long> Clean(ClusterOperationContext context, long index)
        {
            var affectedDatabases = new Dictionary<string, long>();
            foreach (var tuple in ClusterTransactionsCleanup)
            {
                var database = tuple.Key;
                var upToCommandCount = tuple.Value - 1;

                if (ClusterTransactionCommand.DeleteCommands(context, database, upToCommandCount))
                {
                    affectedDatabases.Add(database, tuple.Value);
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
