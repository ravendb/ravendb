using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;

internal class AiIntegrationHandlerProcessorForTestAiConnection<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public AiIntegrationHandlerProcessorForTestAiConnection([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (var token = RequestHandler.CreateTimeLimitedBackgroundOperationToken())
            await AiIntegrationTestConnectionHelper.ExecuteAsync(RequestHandler, token.Token);
    }
}
