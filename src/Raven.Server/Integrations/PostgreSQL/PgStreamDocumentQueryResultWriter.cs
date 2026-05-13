using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL
{
    internal sealed class PgStreamDocumentQueryResultWriter : IStreamQueryResultWriter<Document>
    {
        private readonly PipeWriter _pipeWriter;
        private readonly MessageBuilder _builder;
        private readonly Dictionary<string, PgColumn> _columns;
        private readonly short? _idIndex;
        private readonly short _jsonIndex;
        private readonly ReadOnlyMemory<byte>?[] _row;
        private readonly Action<string, BlittableJsonReaderObject.PropertyDetails, object, ReadOnlyMemory<byte>?[]> _handleSpecialColumns;
        private readonly DocumentsOperationContext _context;

        public bool SupportStatistics => false;
        public int Count { get; private set; }

        public PgStreamDocumentQueryResultWriter(
            PipeWriter pipeWriter,
            MessageBuilder builder,
            Dictionary<string, PgColumn> columns,
            Action<string, BlittableJsonReaderObject.PropertyDetails, object, ReadOnlyMemory<byte>?[]> handleSpecialColumns,
            DocumentsOperationContext context)
        {
            _pipeWriter = pipeWriter;
            _builder = builder;
            _columns = columns;
            _handleSpecialColumns = handleSpecialColumns;
            _context = context;
            _row = ArrayPool<ReadOnlyMemory<byte>?>.Shared.Rent(columns.Count);

            if (columns.TryGetValue(Constants.Documents.Indexing.Fields.DocumentIdFieldName, out var idCol))
                _idIndex = idCol.ColumnIndex;

            _jsonIndex = columns[Constants.Documents.Querying.Fields.PowerBIJsonFieldName].ColumnIndex;
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
            var jsonResult = result.Data;

            // ArrayPool.Rent may return an array larger than _columns.Count; clear only the used portion.
            Array.Clear(_row, 0, _columns.Count);

            if (_idIndex != null && result.Id != null)
                _row[_idIndex.Value] = Encoding.UTF8.GetBytes(result.Id.ToString());

            jsonResult.Modifications = new DynamicJsonValue(jsonResult);

            if (jsonResult.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject _))
                jsonResult.Modifications.Remove(Constants.Documents.Metadata.Key);

            BlittableJsonReaderObject.PropertyDetails prop = default;

            foreach (var (columnName, pgColumn) in _columns)
            {
                var index = jsonResult.GetPropertyIndex(columnName);
                if (index == -1)
                    continue;

                jsonResult.GetPropertyByIndex(index, ref prop);

                _row[pgColumn.ColumnIndex] = RqlQuery.GetValueByType(prop, prop.Value, pgColumn);
                _handleSpecialColumns.Invoke(columnName, prop, prop.Value, _row);

                jsonResult.Modifications.Remove(columnName);
            }

            if (jsonResult.Modifications.Removals.Count != jsonResult.Count)
            {
                using var modified = _context.ReadObject(jsonResult, "renew");
                _row[_jsonIndex] = Encoding.UTF8.GetBytes(modified.ToString());
            }

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
