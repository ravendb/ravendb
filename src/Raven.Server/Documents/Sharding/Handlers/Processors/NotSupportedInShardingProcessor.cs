using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors;

internal sealed class NotSupportedInShardingProcessor : AbstractDatabaseHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    private readonly string _message;

    public NotSupportedInShardingProcessor([NotNull] ShardedDatabaseRequestHandler requestHandler, [NotNull] string message)
        : base(requestHandler)
    {
        _message = message ?? throw new ArgumentNullException(nameof(message));
    }

    public override ValueTask ExecuteAsync()
    {
        throw new NotSupportedInShardingException(_message);
    }
}
