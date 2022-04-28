using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForSource<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractIndexHandlerProcessorForSource([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract IndexInformationHolder GetIndex(string name);

    public override async ValueTask ExecuteAsync()
    {
        var name = GetName();

        var index = GetIndex(name);
        if (index == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        if (index.Type.IsStatic() == false)
            throw new InvalidOperationException("Source can be only retrieved for static indexes.");

        var staticIndex = (StaticIndexInformationHolder)index;
        var source = staticIndex.Compiled.Source;

        if (string.IsNullOrWhiteSpace(source))
            throw new InvalidOperationException("Could not retrieve source for given index.");

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            context.Write(writer, new DynamicJsonValue
            {
                ["Index"] = index.Name,
                ["Source"] = source
            });
        }
    }

    private string GetName()
    {
        return RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
    }
}
