using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Indexes.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal abstract class AbstractAdminIndexHandlerProcessorForTestIndex<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminIndexHandlerProcessorForTestIndex([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    protected async Task<TestIndexParameters> GetTestIndexParametersAsync(JsonOperationContext context)
    {
        var requestBodyStream = RequestHandler.RequestBodyStream();
        
        var input = await context.ReadForMemoryAsync(requestBodyStream, "Input");

        var testIndexParameters = JsonDeserializationServer.TestIndexParameters(input);

        return testIndexParameters;
    }
}
