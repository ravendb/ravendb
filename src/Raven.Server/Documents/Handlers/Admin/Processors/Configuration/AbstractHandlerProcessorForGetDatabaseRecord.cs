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

internal abstract class AbstractHandlerDatabaseProcessorForGetDatabaseRecord<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForGetDatabaseRecord<TRequestHandler>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractHandlerDatabaseProcessorForGetDatabaseRecord([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string DatabaseName => RequestHandler.DatabaseName;
}

internal abstract class AbstractHandlerProcessorForGetDatabaseRecord<TRequestHandler> : AbstractHandlerProcessor<TRequestHandler>
    where TRequestHandler : RequestHandler
{
    protected AbstractHandlerProcessorForGetDatabaseRecord([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract string DatabaseName { get; }

    public override async ValueTask ExecuteAsync()
    {
        var name = DatabaseName;

        using (ClusterContextPool.AllocateOperationContext(out ClusterOperationContext context))
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
