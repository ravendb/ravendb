using System.IO;
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
        public override async ValueTask AddResultAsync(Document res, CancellationToken token)
        {
            // add @id property if res.Id != null, res.Id is null in map-reduce index
            WriteCsvHeaderIfNeeded(res.Data, res.Id != null);

            foreach (var (property, path) in GetProperties())
            {
                if (Constants.Documents.Metadata.Id == property)
                {
                    GetCsvWriter().WriteField(res.Id.ToString());
                }
                else
                {
                    var o = new BlittablePath(path).Evaluate(res.Data);
                    GetCsvWriter().WriteField(o?.ToString());
                }
            }

            await GetCsvWriter().NextRecordAsync();
        }

        public StreamCsvDocumentQueryResultWriter(HttpResponse response, Stream stream, DocumentsOperationContext context, string[] properties = null,
            string csvFileName = "export") : base(response, stream, properties, csvFileName)
        {
        }
    }
}
