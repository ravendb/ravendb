using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Server.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamJsonDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
    {
        private readonly AsyncBlittableJsonTextWriter _writer;
        private readonly JsonOperationContext _context;
        private bool _first = true;

        public StreamJsonDocumentQueryResultWriter(Stream stream, JsonOperationContext context)
        {
            _context = context;
            _writer = new AsyncBlittableJsonTextWriter(context, stream);
        }

        public ValueTask DisposeAsync()
        {
            return _writer.DisposeAsync();
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

        public async ValueTask AddResultAsync(Document res, CancellationToken token)
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
            await _writer.MaybeFlushAsync(token);
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
