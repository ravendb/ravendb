using System.Collections.Generic;
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
        public TimeSeriesHandlerProcessorForGetTimeSeriesStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override ValueTask<TimeSeriesStatistics> GetTimeSeriesStatsAsync(DocumentsOperationContext context, string docId)
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
                    return ValueTask.FromResult<TimeSeriesStatistics>(null);
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

                return ValueTask.FromResult(tsStats);
            }
        }

        private static List<string> GetTimesSeriesNames(Document document)
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
