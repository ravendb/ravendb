using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Configuration;

internal abstract class AbstractHandlerProcessorForGetDatabaseRecord<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractHandlerProcessorForGetDatabaseRecord([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract string GetDatabaseName();

    public override async ValueTask ExecuteAsync()
    {
        var name = GetDatabaseName();

        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var dbId = Constants.Documents.Prefix + name;
            using (context.OpenReadTransaction())
            using (var dbDoc = RequestHandler.ServerStore.Cluster.Read(context, dbId, out long etag))
            {
                if (dbDoc == null)
                {
                    RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    RequestHandler.HttpContext.Response.Headers["Database-Missing"] = name;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                    {
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                ["Type"] = "Error",
                                ["Message"] = "Database " + name + " wasn't found"
                            });
                    }

                    return;
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WriteDocumentPropertiesWithoutMetadata(context, new Document
                    {
                        Data = dbDoc
                    });
                    writer.WriteComma();
                    writer.WritePropertyName("Etag");
                    writer.WriteInteger(etag);
                    writer.WriteEndObject();
                }
            }
        }
    }
}
