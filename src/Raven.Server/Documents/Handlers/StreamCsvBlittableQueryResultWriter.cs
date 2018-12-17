using System.IO;
using Microsoft.AspNetCore.Http;
using Raven.Client.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class StreamCsvBlittableQueryResultWriter : StreamCsvResultWriter<BlittableJsonReaderObject>
    {
        public override void AddResult(BlittableJsonReaderObject res)
        {
            WriteCsvHeaderIfNeeded(res, false);

            foreach ((var property, var path) in GetProperties())
            {
                var o = new BlittablePath(path).Evaluate(res, false);
                GetCsvWriter().WriteField(o?.ToString());
            }

            GetCsvWriter().NextRecord();
        }

        public StreamCsvBlittableQueryResultWriter(HttpResponse response, Stream stream, DocumentsOperationContext context, string[] properties = null,
            string csvFileName = "export") : base(response, stream, context, properties, csvFileName)
        {
        }
    }
}
