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
    public class StreamJsonlDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
    {
        private readonly AsyncBlittableJsonTextWriter _writer;
        private readonly JsonOperationContext _context;

        public StreamJsonlDocumentQueryResultWriter(Stream stream, JsonOperationContext context)
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
        }

        public void StartResults()
        {
        }

        public void EndResults()
        {
        }

        public async ValueTask AddResultAsync(Document res, CancellationToken token)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Item");
            _writer.WriteDocument(_context, res, metadataOnly: false);
            _writer.WriteEndObject();

            _writer.WriteNewLine();
            await _writer.MaybeFlushAsync(token);
        }

        public void EndResponse()
        {
        }

        public async ValueTask WriteErrorAsync(Exception e)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Error");
            _writer.WriteString(e.ToString());
            _writer.WriteEndObject();

            _writer.WriteNewLine();

            await _writer.FlushAsync();
        }

        public async ValueTask WriteErrorAsync(string error)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Error");
            _writer.WriteString(error);
            _writer.WriteEndObject();

            _writer.WriteNewLine();

            await _writer.FlushAsync();
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Stats");
            _writer.WriteStartObject();

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

            _writer.WriteEndObject();
            _writer.WriteEndObject();

            _writer.WriteNewLine();
        }

        public bool SupportError => true;
        public bool SupportStatistics => true;
    }
}
