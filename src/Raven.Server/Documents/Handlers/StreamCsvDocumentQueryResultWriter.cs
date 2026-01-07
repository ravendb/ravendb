using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public sealed class StreamCsvDocumentQueryResultWriter : StreamCsvResultWriter<Document>
    {
        protected override (string, string)[] GetProperties(Document entity, bool writeIds)
        {
            var properties = GetPropertiesRecursive((string.Empty, string.Empty), entity.Data, writeIds);
            using var tsCsvWriter = new TimeSeriesCsvWriter(entity.TimeSeriesStream);
            {
                return properties.Concat(tsCsvWriter.GetProperties()).ToArray();
            }
        }

        public override async ValueTask AddResultAsync(Document res, CancellationToken token)
        {
            // add @id property if res.Id != null, res.Id is null in map-reduce index
            WriteCsvHeaderIfNeeded(res, res.Id != null);

            using var tsCsvWriter = new TimeSeriesCsvWriter(res.TimeSeriesStream);
            do
            {
                CsvWriter csvWriter = GetCsvWriter();
                foreach (var (property, path) in GetProperties())
                {
                    if (Constants.Documents.Metadata.Id == property)
                    {
                        csvWriter.WriteField(res.Id.ToString(), shouldQuote: true);
                    }
                    else
                    {
                        var o = path.StartsWith(TimeSeriesCsvWriter.TimeSeriesPathPrefix) 
                            ? tsCsvWriter.GetValue(property) 
                            : new BlittablePath(path).Evaluate(res.Data);

                        if (WellKnownTypeForQuoting(o))
                            csvWriter.WriteField(o?.ToString(), shouldQuote: true);
                        else
                            csvWriter.WriteField(o?.ToString());
                    }
                }

                await csvWriter.NextRecordAsync();
            } while (tsCsvWriter.MoveNext());
        }

        public StreamCsvDocumentQueryResultWriter(HttpResponse response, Stream stream, string[] properties = null,
            string csvFileName = "export") : base(response, stream, properties, csvFileName)
        {
        }
    }
}
