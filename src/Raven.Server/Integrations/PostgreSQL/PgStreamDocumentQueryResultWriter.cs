using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL
{
    internal sealed class PgStreamDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
    {
        private readonly PipeWriter _pipeWriter;
        private readonly MessageBuilder _builder;
        private readonly RqlQuery _query;
        private readonly Dictionary<string, PgColumn> _columns;
        private readonly short? _idIndex;
        private readonly short? _jsonIndex;
        private readonly DocumentsOperationContext _context;
        private readonly ReadOnlyMemory<byte>?[] _row;

        public bool SupportStatistics => false;
        public int Count { get; private set; }

        public PgStreamDocumentQueryResultWriter(
            PipeWriter pipeWriter,
            MessageBuilder builder,
            RqlQuery query,
            Dictionary<string, PgColumn> columns,
            short? idIndex,
            short? jsonIndex,
            DocumentsOperationContext context)
        {
            _pipeWriter = pipeWriter;
            _builder = builder;
            _query = query;
            _columns = columns;
            _idIndex = idIndex;
            _jsonIndex = jsonIndex;
            _context = context;
            _row = ArrayPool<ReadOnlyMemory<byte>?>.Shared.Rent(columns.Count);
        }

        public void StartResponse() { }
        public void StartResults() { }
        public void EndResults() { }
        public void EndResponse() { }
        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp) { }
        public ValueTask WriteErrorAsync(Exception e) => default;
        public ValueTask WriteErrorAsync(string error) => default;

        public async ValueTask AddResultAsync(Document result, CancellationToken token)
        {
            // ArrayPool.Rent may return an array larger than _columns.Count; clear only the used portion.
            Array.Clear(_row, 0, _columns.Count);

            _query.WriteRow(result, _row, _idIndex, _jsonIndex, _context);

            await _pipeWriter.WriteAsync(_builder.DataRow(_row[.._columns.Count]), token);
            Count++;
        }

        public ValueTask DisposeAsync()
        {
            ArrayPool<ReadOnlyMemory<byte>?>.Shared.Return(_row, clearArray: true);
            return default;
        }
    }
}
