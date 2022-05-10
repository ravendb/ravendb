using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal class TimeSeriesHandlerProcessorForGetTimeSeriesStats : AbstractTimeSeriesHandlerProcessorForGetTimeSeriesStats<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TimeSeriesHandlerProcessorForGetTimeSeriesStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetTimeSeriesStatsAndWriteAsync(DocumentsOperationContext context, string docId)
        {
            using (context.OpenReadTransaction())
            {
                var tsStats = new TimeSeriesStatistics()
                {
                    DocumentId = docId,
                    TimeSeries = new()
                };

                var document = RequestHandler.Database.DocumentsStorage.Get(context, docId, DocumentFields.Data);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var timeSeriesNames = GetTimesSeriesNames(document);
                
                foreach (var name in timeSeriesNames)
                {
                    var (count, start, end) = RequestHandler.Database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(context, docId, name);
                    tsStats.TimeSeries.Add(new TimeSeriesItemDetail()
                    {
                        Name = name, 
                        NumberOfEntries = count, 
                        StartDate = start, 
                        EndDate = end
                    });
                }

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

        public static List<string> GetTimesSeriesNames(Document document)
        {
            var timeSeriesNames = new List<string>();
            if (document.TryGetMetadata(out var metadata))
            {
                if (metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray timeSeries) && timeSeries != null)
                {
                    foreach (object name in timeSeries)
                    {
                        if (name == null)
                            continue;

                        if (name is string || name is LazyStringValue || name is LazyCompressedStringValue)
                        {
                            timeSeriesNames.Add(name.ToString());
                        }
                    }
                }
            }

            return timeSeriesNames;
        }
    }
}
