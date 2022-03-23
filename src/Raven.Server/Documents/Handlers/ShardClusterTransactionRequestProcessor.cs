using System;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Web;

namespace Raven.Server.Documents.Handlers;

class ShardClusterTransactionRequestProcessor : BaseClusterTransactionRequestProcessor
{
    public ShardClusterTransactionRequestProcessor(RequestHandler handler, string database, char identitySeparator) : base(handler, database, identitySeparator)
    {
    }

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
