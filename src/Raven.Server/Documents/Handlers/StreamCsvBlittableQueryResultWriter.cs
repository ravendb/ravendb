using System.IO;
using Microsoft.AspNetCore.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StreamCsvBlittableQueryResultWriter : StreamCsvResultWriter<BlittableJsonReaderObject>
    {
        public override void AddResult(BlittableJsonReaderObject res)
        {
            WriteCsvHeaderIfNeeded(res, false);

            foreach (var (_, path) in GetProperties())
            {
                var o = new BlittablePath(path).Evaluate(res, false);
                GetCsvWriter().WriteField(o?.ToString());
            }

            GetCsvWriter().NextRecord();
        }

        public StreamCsvBlittableQueryResultWriter(HttpResponse response, Stream stream, string[] properties = null,
            string csvFileName = "export") : base(response, stream, properties, csvFileName)
        {
        }
    }
}
