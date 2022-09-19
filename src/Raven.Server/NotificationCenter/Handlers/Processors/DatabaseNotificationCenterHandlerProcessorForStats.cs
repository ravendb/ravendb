using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter.Handlers.Processors;

internal class DatabaseNotificationCenterHandlerProcessorForStats : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public DatabaseNotificationCenterHandlerProcessorForStats([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var stats = DatabaseStatsSender.GetStats(RequestHandler.Database);

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(stats.CountOfDocuments));
            writer.WriteInteger(stats.CountOfDocuments);

            writer.WriteEndObject();
        }
    }
}
