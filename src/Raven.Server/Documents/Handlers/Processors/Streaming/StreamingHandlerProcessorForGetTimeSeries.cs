using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal class StreamingHandlerProcessorForGetTimeSeries : AbstractStreamingHandlerProcessorForGetTimeSeries<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public StreamingHandlerProcessorForGetTimeSeries([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IDisposable OpenReadTransaction(DocumentsOperationContext context)
        {
            return context.OpenReadTransaction();
        }

        protected override async ValueTask GetAndWriteTimeSeriesAsync(DocumentsOperationContext context, string docId, string name, DateTime @from, DateTime to, TimeSpan? offset, CancellationToken token)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var reader = new TimeSeriesReader(context, docId, name, from, to, offset, token);

                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteStartArray();

                foreach (var entry in reader.AllValues())
                {
                    context.Write(writer, entry.ToTimeSeriesEntryJson());
                    writer.WriteComma();
                    await writer.MaybeFlushAsync(token);
                }

                writer.WriteEndArray();
                writer.WriteEndObject();

                await writer.MaybeFlushAsync(token);
            }
        }
    }
}
