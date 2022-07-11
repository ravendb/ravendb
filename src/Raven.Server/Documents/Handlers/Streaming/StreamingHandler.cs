using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Streaming
{
    public class StreamingHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/streams/docs", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamDocsGet()
        {
            using (var processor = new StreamingHandlerProcessorForGetDocs(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/streams/timeseries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Stream()
        {
            var documentId = GetStringQueryString("docId");
            var name = GetStringQueryString("name");
            var fromStr = GetStringQueryString("from", required: false);
            var toStr = GetStringQueryString("to", required: false);
            var offset = GetTimeSpanQueryString("offset", required: false);

            var from = string.IsNullOrEmpty(fromStr)
                ? DateTime.MinValue
                : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(fromStr, name);

            var to = string.IsNullOrEmpty(toStr)
                ? DateTime.MaxValue
                : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(toStr, name);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                using (var token = CreateOperationToken())
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var reader = new TimeSeriesReader(context, documentId, name, from, to, offset, token.Token);

                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    
                    foreach (var entry in reader.AllValues())
                    {
                        context.Write(writer, entry.ToTimeSeriesEntryJson());
                        writer.WriteComma();
                        await writer.MaybeFlushAsync(token.Token);
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();

                    await writer.MaybeFlushAsync(token.Token);
                }
            }
        }

        [RavenAction("/databases/*/streams/queries", "HEAD", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task SteamQueryHead()
        {
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/streams/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamQueryGet()
        {
            using (var processor = new StreamingHandlerProcessorForGetStreamQuery(this, HttpMethod.Get))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/streams/queries", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task StreamQueryPost()
        {
            using (var processor = new StreamingHandlerProcessorForGetStreamQuery(this, HttpMethod.Post))
                await processor.ExecuteAsync();
        }
    }
}
