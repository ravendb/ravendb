using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class StreamCsvDocumentQueryResultWriter : StreamCsvResultWriter<Document>
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
                foreach (var (property, path) in GetProperties())
                {
                    if (Constants.Documents.Metadata.Id == property)
                    {
                        GetCsvWriter().WriteField(res.Id.ToString());
                    }
                    else
                    {
                        var o = path.StartsWith(TimeSeriesCsvWriter.TimeSeriesPathPrefix) ? 
                            tsCsvWriter.GetValue(property) : 
                            new BlittablePath(path).Evaluate(res.Data);

                        GetCsvWriter().WriteField(o?.ToString());
                    }
                }
                await GetCsvWriter().NextRecordAsync();

            } while (tsCsvWriter.MoveNext());
        }

        public StreamCsvDocumentQueryResultWriter(HttpResponse response, Stream stream, DocumentsOperationContext context, string[] properties = null,
            string csvFileName = "export") : base(response, stream, properties, csvFileName)
        {
        }
    }
}
