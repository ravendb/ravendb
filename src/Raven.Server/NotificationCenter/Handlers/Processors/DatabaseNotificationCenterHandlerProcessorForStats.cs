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
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.CountOfConflicts));
            writer.WriteInteger(stats.CountOfConflicts);
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.LastEtag));
            writer.WriteInteger(stats.LastEtag);
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.CountOfIndexes));
            writer.WriteInteger(stats.CountOfIndexes);
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.CountOfStaleIndexes));
            writer.WriteInteger(stats.CountOfStaleIndexes);
            writer.WriteComma();

            writer.WriteArray(nameof(stats.StaleIndexes), stats.StaleIndexes);
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.CountOfIndexingErrors));
            writer.WriteInteger(stats.CountOfIndexingErrors);
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.GlobalChangeVector));
            writer.WriteString(stats.GlobalChangeVector);
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.LastIndexingErrorTime));
            writer.WriteDateTime(stats.LastIndexingErrorTime, isUtc: true);
            writer.WriteComma();

            writer.WritePropertyName(nameof(stats.Collections));
            if (stats.Collections == null)
            {
                writer.WriteNull();
            }
            else
            {
                writer.WriteStartObject();

                var first = true;
                foreach (var kvp in stats.Collections)
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    writer.WritePropertyName(kvp.Key);

                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(kvp.Value.Name));
                    writer.WriteString(kvp.Value.Name);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(kvp.Value.Count));
                    writer.WriteInteger(kvp.Value.Count);
                    writer.WriteComma();
                    
                    writer.WritePropertyName(nameof(kvp.Value.LastDocumentChangeVector));
                    writer.WriteString(kvp.Value.LastDocumentChangeVector);

                    writer.WriteEndObject();
                }

                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }
    }
}
