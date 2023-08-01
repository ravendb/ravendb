using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;

namespace Raven.Server.Documents.Sharding
{
    public partial class ShardedDatabaseContext
    {
        public readonly ShardedSmugglerContext Smuggler;

        public sealed class ShardedSmugglerContext
        {
            private readonly ShardedDatabaseContext _context;

            private readonly ServerStore _serverStore;

            public ShardedSmugglerContext([NotNull] ShardedDatabaseContext context, ServerStore serverStore)
            {
                _context = context;
                _serverStore = serverStore;
            }

            public ImportDelegate GetImportDelegateForHandler(ShardedDatabaseRequestHandler handler)
            {
                return async (context, stream, options, result, onProgress, operationId, token) =>
                {
                    using (var source = new OrchestratorStreamSource(stream, context, _context.DatabaseName, _context.ShardCount, options))
                    {
                        DatabaseRecord record;
                        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            record = _serverStore.Cluster.ReadDatabase(ctx, _context.DatabaseName);
                        }

                        var destination = new MultiShardedDestination(source, _context, handler, operationId);
                        var smuggler = new ShardedDatabaseSmuggler(source, destination, context, record, _serverStore, options, result, onProgress, token.Token);

                        return await smuggler.ExecuteAsync();
                    }
                };
            }
        }
    }
}
