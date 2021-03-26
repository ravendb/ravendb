using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StreamCsvBlittableQueryResultWriter : StreamCsvResultWriter<BlittableJsonReaderObject>
    {
        public override async ValueTask AddResultAsync(BlittableJsonReaderObject res, CancellationToken token)
        {
            WriteCsvHeaderIfNeeded(res, false);

            foreach (var (_, path) in GetProperties())
            {
                var o = new BlittablePath(path).Evaluate(res);
                GetCsvWriter().WriteField(o?.ToString());
            }

            await GetCsvWriter().NextRecordAsync();
        }

        public StreamCsvBlittableQueryResultWriter(HttpResponse response, Stream stream, string[] properties = null,
            string csvFileName = "export") : base(response, stream, properties, csvFileName)
        {
        }
    }
}
