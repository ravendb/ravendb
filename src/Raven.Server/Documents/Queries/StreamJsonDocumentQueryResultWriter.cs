using System;
using System.IO;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Session;
using Raven.Server.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamJsonDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
    {
        private BlittableJsonTextWriter _writer;
        private HttpResponse _response;
        private JsonOperationContext _context;
        private bool _first = true;

        public StreamJsonDocumentQueryResultWriter(HttpResponse response, Stream stream, JsonOperationContext context)
        {
            _context = context;
            _writer = new BlittableJsonTextWriter(context, stream);
            _response = response;
        }

        public void Dispose()
        {
            _writer.Dispose();
        }

        public void StartResponse()
        {
            _writer.WriteStartObject();
        }

        public void StartResults()
        {
            _writer.WritePropertyName("Results");
            _writer.WriteStartArray();
        }

        public void EndResults()
        {
            _writer.WriteEndArray();
        }

        public void AddResult(Document res)
        {
            if (_first == false)
            {
                _writer.WriteComma();
            }
            else
            {
                _first = false;
            }
            _writer.WriteDocument(_context, res, metadataOnly: false);
        }

        public void EndResponse()
        {
            _writer.WriteEndObject();
        }

        public void WriteError(Exception e)
        {
            _writer.WriteComma();

            _writer.WritePropertyName("Error");
            _writer.WriteString(e.ToString());
        }

        public void WriteError(string error)
        {            
            _writer.WritePropertyName("Error");
            _writer.WriteString(error);
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            _writer.WritePropertyName(nameof(StreamQueryStatistics.ResultEtag));
            _writer.WriteInteger(resultEtag);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.IsStale));
            _writer.WriteBool(isStale);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.IndexName));
            _writer.WriteString(indexName);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.TotalResults));
            _writer.WriteInteger(totalResults);
            _writer.WriteComma();

            _writer.WritePropertyName(nameof(StreamQueryStatistics.IndexTimestamp));
            _writer.WriteString(timestamp.GetDefaultRavenFormat(isUtc: true));
            _writer.WriteComma();
        }

        public bool SupportError => true;
        public bool SupportStatistics => true;
    }
}
