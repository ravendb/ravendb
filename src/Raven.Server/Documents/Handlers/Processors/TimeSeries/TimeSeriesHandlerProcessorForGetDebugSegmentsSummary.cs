using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal class TimeSeriesHandlerProcessorForGetDebugSegmentsSummary : AbstractTimeSeriesHandlerProcessorForGetDebugSegmentsSummary<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TimeSeriesHandlerProcessorForGetDebugSegmentsSummary([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask GetSegmentsSummaryAndWriteAsync(DocumentsOperationContext context, string docId, string name, DateTime @from, DateTime to)
        {
            using (context.OpenReadTransaction())
            {
                var segmentsSummary = RequestHandler.Database.DocumentsStorage.TimeSeriesStorage.GetSegmentsSummary(context, docId, name, from, to);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var first = true;
                    foreach (var summery in segmentsSummary)
                    {
                        if (!first)
                            writer.WriteComma();
                        context.Write(writer, summery.ToJson());
                        first = false;
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }
        }
    }
}
