using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Handlers.Processors.Stats;

internal abstract class
    AbstractStatsHandlerProcessorForGetValidateUnusedIds<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler,
        TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStatsHandlerProcessorForGetValidateUnusedIds([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var database = RequestHandler.DatabaseName;
        await ServerStore.EnsureNotPassiveAsync();

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var json = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), "unused-databases-ids"))
        {
            var parameters = JsonDeserializationServer.Parameters.ValidateUnusedIdsParameters(json);
            using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken(ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan))
                await ValidateUnusedIdsOnAllNodesAsync(parameters.DatabaseIds, database, token.Token);
        }
    }

    protected abstract Task ValidateUnusedIdsOnAllNodesAsync(HashSet<string> unusedIds,
        string databaseName, CancellationToken token);
}
