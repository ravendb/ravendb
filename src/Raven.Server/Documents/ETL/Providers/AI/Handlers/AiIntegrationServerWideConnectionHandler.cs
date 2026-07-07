using System;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.AI.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Web;

namespace Raven.Server.Documents.ETL.Providers.AI.Handlers;

public sealed class AiIntegrationServerWideConnectionHandler : ServerRequestHandler
{
    [RavenAction("/admin/ai/test-connection", "POST", AuthorizationStatus.Operator)]
    public async Task TestAiConnection()
    {
        using (var token = new OperationCancelToken(TimeSpan.FromMinutes(2), ServerStore.ServerShutdown, HttpContext.RequestAborted))
            await AiIntegrationTestConnectionHelper.ExecuteAsync(this, token.Token);
    }
}
