using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    private readonly HttpStatusCode _statusCode;

    protected AbstractHandlerProxyNoContentProcessor([NotNull] TRequestHandler requestHandler, HttpStatusCode statusCode = HttpStatusCode.NoContent) : base(requestHandler)
    {
        _statusCode = statusCode;
    }

    public override async ValueTask ExecuteAsync()
    {
        await base.ExecuteAsync();

        RequestHandler.NoContentStatus(_statusCode);
    }
}
