using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsBinCleanerHandlerProcessorForPostConfiguration<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractRevisionsBinCleanerHandlerProcessorForPostConfiguration([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configurationJson, string raftRequestId)
        {
            var database = RequestHandler.DatabaseName;

            var config = JsonDeserializationCluster.RevisionsBinConfiguration(configurationJson);
            if(config.MaxItemsToProcess <= 0)
                throw new InvalidOperationException(
                    $"RevisionsBinConfiguration.MaxItemsToProcess for database '{database}' must be greater than 0.");

            if (config.NumberOfDeletesInBatch <= 0)
                throw new InvalidOperationException(
                    $"RevisionsBinConfiguration.NumberOfDeletesInBatch for database '{database}' must be greater than 0.");

            var cmd = new RevisionsBinConfigurationCommand(config, database, raftRequestId);
            return ServerStore.SendToLeaderAsync(cmd);
        }
    }
}
