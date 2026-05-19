using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForRetryBatch : AbstractEtlHandlerProcessorForRetryBatch<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForRetryBatch(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var etlProcessName = GetEtlProcessName();

        var process = RequestHandler.Database.EtlLoader.Processes.SingleOrDefault(x => x.Name == etlProcessName);
        if (process == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return ValueTask.CompletedTask;
        }

        process.ForceBatchRetry();

        return ValueTask.CompletedTask;
    }
    
    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
