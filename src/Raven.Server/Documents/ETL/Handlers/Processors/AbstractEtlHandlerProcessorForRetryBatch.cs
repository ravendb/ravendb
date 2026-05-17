using System.Diagnostics.CodeAnalysis;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal abstract class AbstractEtlHandlerProcessorForRetryBatch<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractEtlHandlerProcessorForRetryBatch([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
    {
        var etlProcessName = GetEtlProcessName();

        return new RetryBatchCommand(nodeTag, etlProcessName);
    }

    protected string GetEtlProcessName() => RequestHandler.GetStringQueryString("name");
}
