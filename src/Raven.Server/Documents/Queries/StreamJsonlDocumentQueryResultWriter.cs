using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Json;
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
            _writer.WriteDocument(_context, res, metadataOnly: false);
            _writer.WriteNewLine();
            await _writer.MaybeFlushAsync(token);
        }

        public void EndResponse()
        {
        }

        public ValueTask WriteErrorAsync(Exception e)
        {
            throw new NotSupportedException();
        }

        public ValueTask WriteErrorAsync(string error)
        {
            throw new NotSupportedException();
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            throw new NotSupportedException();
        }

        public bool SupportError => false;
        public bool SupportStatistics => false;
    }
}
