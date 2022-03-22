using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioDatabaseTasksHandlerProcessorForGetIndexDefaults<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractStudioDatabaseTasksHandlerProcessorForGetIndexDefaults([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract RavenConfiguration GetDatabaseConfiguration();

    public override async ValueTask ExecuteAsync()
    {
        var configuration = GetDatabaseConfiguration();

        var autoIndexesDeploymentMode = configuration.Indexing.AutoIndexDeploymentMode;
        var staticIndexesDeploymentMode = configuration.Indexing.StaticIndexDeploymentMode;

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(IndexDefaults.AutoIndexDeploymentMode));
            writer.WriteString(autoIndexesDeploymentMode.ToString());
            writer.WriteComma();
            writer.WritePropertyName(nameof(IndexDefaults.StaticIndexDeploymentMode));
            writer.WriteString(staticIndexesDeploymentMode.ToString());
            writer.WriteEndObject();
        }
    }
}

public class IndexDefaults
{
    public IndexDeploymentMode AutoIndexDeploymentMode { get; set; }
    public IndexDeploymentMode StaticIndexDeploymentMode { get; set; }
}
