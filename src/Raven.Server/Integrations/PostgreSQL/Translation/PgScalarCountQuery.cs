using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Translation
{
    // Answers `SELECT count(*) FROM t [WHERE ...]` with a single int8 row (what PostgreSQL's count()
    // returns). Page size 0 makes the engine drain the whole filtered enumerator and report the exact
    // TotalResults - the same mechanism as IDocumentQueryBase.Count(). The buffered ExecuteQuery path
    // is mandatory: ExecuteStreamQuery never surfaces TotalResults.
    internal sealed class PgScalarCountQuery : PgQuery
    {
        private readonly DocumentDatabase _documentDatabase;
        private readonly string[] _columnNames;
        private readonly List<PgColumn> _columns = new();

        public PgScalarCountQuery(string rql, int[] parametersDataTypes, DocumentDatabase documentDatabase, string[] columnNames)
            : base(rql, parametersDataTypes)
        {
            _documentDatabase = documentDatabase;
            _columnNames = columnNames;
        }

        public override Task<ICollection<PgColumn>> Init()
        {
            var resultsFormat = GetDefaultResultsFormat();

            _columns.Clear();
            for (int i = 0; i < _columnNames.Length; i++)
                _columns.Add(new PgColumn(_columnNames[i], (short)i, PgInt8.Default, resultsFormat));

            return Task.FromResult<ICollection<PgColumn>>(_columns);
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            if (_columns.Count == 0)
                await Init();

            var count = await GetCount(token);

            var row = new ReadOnlyMemory<byte>?[_columns.Count];
            for (int i = 0; i < row.Length; i++)
                row[i] = PgInt8.Default.ToBytes(count, _columns[i].FormatCode);

            await writer.WriteAsync(builder.DataRow(row), token);
            await writer.WriteAsync(builder.CommandComplete("SELECT 1"), token);
        }

        private async Task<long> GetCount(CancellationToken token)
        {
            using var queryOperationContext = QueryOperationContext.Allocate(_documentDatabase);

            var parameters = DynamicJsonValue.Convert(Parameters);
            var queryParameters = queryOperationContext.Documents.ReadObject(parameters, "query/parameters");

            var indexQuery = new IndexQueryServerSide(QueryString, queryParameters)
            {
                Start = 0,
                PageSize = 0,
                Offset = 0,
                Limit = 0
            };

            using var cancelToken = new OperationCancelToken(_documentDatabase.DatabaseShutdown, token);
            var result = await _documentDatabase.QueryRunner.ExecuteQuery(indexQuery, queryOperationContext, null, cancelToken);

            return result.TotalResults;
        }

        public override void Dispose()
        {
        }
    }
}
