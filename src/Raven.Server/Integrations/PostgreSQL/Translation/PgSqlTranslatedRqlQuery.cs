using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.Translation
{
    // A translated-RQL query that suppresses the synthetic id/json columns base RqlQuery adds by
    // default, so the response matches the columns the client named, with no extra json blob. Two
    // callers route here: the SQL translator's explicit-projection case (`SELECT col1, col2 FROM t`;
    // `SELECT *` stays on plain RqlQuery) and PowerBI's narrow-projection Fetch shapes. Const
    // projections (`1 as "c0"`) are supported so a narrow+const shape lands here, not on PowerBIRqlQuery.
    internal sealed class PgSqlTranslatedRqlQuery : RqlQuery
    {
        private readonly IReadOnlyList<ConstProjection> _constProjections;

        public PgSqlTranslatedRqlQuery(string queryString, int[] parametersDataTypes, DocumentDatabase documentDatabase, int? limit = null, IReadOnlyList<ConstProjection> constProjections = null)
            : base(queryString, parametersDataTypes, documentDatabase, limit)
        {
            _constProjections = constProjections is { Count: > 0 } ? constProjections : null;
        }

        protected override bool IncludeDocumentIdColumn => false;

        protected override bool IncludePowerBIJsonColumn => false;

        protected override async Task<ICollection<PgColumn>> GenerateSchema()
        {
            await base.GenerateSchema();

            if (_constProjections != null)
            {
                foreach (var cp in _constProjections)
                {
                    if (string.IsNullOrWhiteSpace(cp.ColumnName))
                        continue;
                    if (Columns.ContainsKey(cp.ColumnName))
                        continue;
                    Columns[cp.ColumnName] = new PgColumn(cp.ColumnName, (short)Columns.Count, cp.PgType, GetDefaultResultsFormat());
                }
            }

            return Columns.Values;
        }

        protected override void AfterRow(BlittableJsonReaderObject jsonResult, ReadOnlyMemory<byte>?[] row, short? jsonIndex, DocumentsOperationContext context)
        {
            base.AfterRow(jsonResult, row, jsonIndex, context);

            if (_constProjections == null)
                return;

            foreach (var cp in _constProjections)
            {
                if (cp.Value == null)
                    continue;
                if (Columns.TryGetValue(cp.ColumnName, out var col) == false)
                    continue;
                row[col.ColumnIndex] = cp.PgType.ToBytes(cp.Value, col.FormatCode);
            }
        }
    }
}
