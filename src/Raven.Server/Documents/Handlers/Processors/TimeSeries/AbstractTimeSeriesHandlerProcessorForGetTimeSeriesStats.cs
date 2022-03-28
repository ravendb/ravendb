using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal abstract class AbstractTimeSeriesHandlerProcessorForGetTimeSeriesStats<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractTimeSeriesHandlerProcessorForGetTimeSeriesStats([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask<TimeSeriesStatistics> GetTimeSeriesStatsAsync(string docId);

        public override async ValueTask ExecuteAsync()
        {
            var documentId = RequestHandler.GetStringQueryString("docId");

            var tsStats = await GetTimeSeriesStatsAsync(documentId);

            if (tsStats == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(TimeSeriesStatistics.DocumentId));
                    writer.WriteString(tsStats.DocumentId);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(TimeSeriesStatistics.TimeSeries));

                    writer.WriteStartArray();

                    var first = true;
                    foreach (var details in tsStats.TimeSeries)
                    {
                        if (first == false)
                        {
                            writer.WriteComma();
                        }
                        first = false;

                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.Name));
                        writer.WriteString(details.Name);

                        writer.WriteComma();

                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.NumberOfEntries));
                        writer.WriteInteger(details.NumberOfEntries);

                        writer.WriteComma();

                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.StartDate));
                        writer.WriteDateTime(details.StartDate, isUtc: true);

                        writer.WriteComma();

                        writer.WritePropertyName(nameof(TimeSeriesItemDetail.EndDate));
                        writer.WriteDateTime(details.EndDate, isUtc: true);

                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
        }
    }
}
