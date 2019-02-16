using System.IO;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Json;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    public class StreamCsvDocumentQueryResultWriter : StreamCsvResultWriter<Document>
    {
        public override void AddResult(Document res)
        {
            WriteCsvHeaderIfNeeded(res.Data);

            foreach (var (property, path) in GetProperties())
            {
                if (Constants.Documents.Metadata.Id == property)
                {
                    GetCsvWriter().WriteField(res.Id);
                }
                else
                {
                    var o = new BlittablePath(path).Evaluate(res.Data, false);
                    GetCsvWriter().WriteField(o?.ToString());
                }
            }
            GetCsvWriter().NextRecord();
        }

        public StreamCsvDocumentQueryResultWriter(HttpResponse response, Stream stream, DocumentsOperationContext context, string[] properties = null,
            string csvFileName = "export") : base(response, stream, properties, csvFileName)
        {

        }
    }
}
