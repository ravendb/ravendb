using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal class StudioIndexHandlerForPostIndexType<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public StudioIndexHandlerForPostIndexType([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            using (var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "map"))
            {
                var indexDefinition = JsonDeserializationServer.IndexDefinition(json);

                var indexType = indexDefinition.DetectStaticIndexType();
                var indexSourceType = indexDefinition.DetectStaticIndexSourceType();

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(IndexTypeInfo.IndexType));
                    writer.WriteString(indexType.ToString());
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(IndexTypeInfo.IndexSourceType));
                    writer.WriteString(indexSourceType.ToString());
                    writer.WriteEndObject();
                }
            }
        }
    }
}

public class IndexTypeInfo
{
    public IndexType IndexType { get; set; }
    public IndexSourceType IndexSourceType { get; set; }
}
