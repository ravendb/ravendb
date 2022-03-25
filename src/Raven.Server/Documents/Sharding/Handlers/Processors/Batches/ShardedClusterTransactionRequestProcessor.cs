using System;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Web;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Batches;

public class ShardedClusterTransactionRequestProcessor : AbstractClusterTransactionRequestProcessor<ShardedBatchCommand>
{
    public ShardedClusterTransactionRequestProcessor(RequestHandler handler, string database, char identitySeparator) : base(handler, database, identitySeparator)
    {
    }

    protected override ArraySegment<BatchRequestParser.CommandData> GetParsedCommands(ShardedBatchCommand command) => command.ParsedCommands;

    protected override ClusterTransactionCommand CreateClusterTransactionCommand(
        ArraySegment<BatchRequestParser.CommandData> parsedCommands,
        ClusterTransactionCommand.ClusterTransactionOptions options,
        string raftRequestId)
    {
        return new ClusterTransactionCommand(
            Database,
            IdentitySeparator,
            parsedCommands,
            options,
            raftRequestId);
    }
}
