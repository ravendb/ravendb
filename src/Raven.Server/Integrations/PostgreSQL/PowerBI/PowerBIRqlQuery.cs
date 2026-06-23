using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.PowerBI
{
    public class PowerBIRqlQuery : RqlQuery
    {
        private readonly Dictionary<string, ReplaceColumnValue> _replaces;
        private readonly IReadOnlyList<ConstProjection> _constProjections;

        public PowerBIRqlQuery(string queryString, int[] parametersDataTypes, DocumentDatabase documentDatabase, Dictionary<string, ReplaceColumnValue> replaces = null, int? limit = null, IReadOnlyList<ConstProjection> constProjections = null) : base(queryString, parametersDataTypes, documentDatabase, limit)
        {
            _replaces = replaces;
            _constProjections = constProjections is { Count: > 0 } ? constProjections : null;
        }

        protected override async Task<ICollection<PgColumn>> GenerateSchema()
        {
            await base.GenerateSchema();

            // Append outer-wrapper const projections AFTER base's synthetic json - PowerBI's
            // SQL puts them after json in its projection list, and wire-order mismatch shows
            // up client-side as DISP_E_TYPEMISMATCH or silent data corruption.
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

            if (_replaces == null)
                return Columns.Values;

            var orderedReplaces = _replaces.Values
                .OrderBy(x => x.SrcColumnName, StringComparer.OrdinalIgnoreCase)
                .ThenBy(x => x.DstColumnName, StringComparer.OrdinalIgnoreCase)
                .ToList();

            // The longest possible chain is bounded by the number of replace entries.
            for (var i = 0; i < _replaces.Count; i++)
            {
                var added = false;

                foreach (var replace in orderedReplaces)
                {
                    if (string.IsNullOrEmpty(replace.DstColumnName))
                        continue;

                    if (string.Equals(replace.SrcColumnName, replace.DstColumnName, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (Columns.ContainsKey(replace.DstColumnName))
                        continue;

                    if (Columns.TryGetValue(replace.SrcColumnName, out var originalColumn) == false)
                        continue;

                    if (Columns.TryAdd(replace.DstColumnName,
                            new PgColumn(replace.DstColumnName, (short)Columns.Count, originalColumn.PgType, originalColumn.FormatCode)))
                    {
                        added = true;
                    }
                }

                if (added == false)
                    break;
            }

            return Columns.Values;
        }

        protected override void AfterRow(BlittableJsonReaderObject jsonResult, ReadOnlyMemory<byte>?[] row, short? jsonIndex, DocumentsOperationContext context)
        {
            base.AfterRow(jsonResult, row, jsonIndex, context);

            // Encode each const projection's literal at the column's PgType/format code. A null Value is
            // left as the array's default slot, which the protocol writer emits as wire NULL.
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

        protected override void HandleSpecialColumnsIfNeeded(string columnName, BlittableJsonReaderObject.PropertyDetails property, object value, ReadOnlyMemory<byte>?[] row)
        {
            if (_replaces == null)
                return;

            if (_replaces.TryGetValue(columnName, out var replace))
            {
                var replaceColumn = Columns[replace.DstColumnName];

                object replacedValue = null;

                switch (property.Token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.String:
                    case BlittableJsonToken.StartArray:
                    case BlittableJsonToken.StartObject:
                    case BlittableJsonToken.CompressedString:
                    case BlittableJsonToken.EmbeddedBlittable:
                        if (value != null)
                        {
                            var columnValue = value.ToString();

                            replacedValue = columnValue?.Replace(replace.OldValue?.ToString() ?? string.Empty, replace.NewValue.ToString());
                        }
                        break;
                }

                // null nullable -> MessageBuilder.DataRow writes length -1 = wire NULL.
                // An empty-but-present byte array writes length 0 = empty string, which
                // for non-string columns (e.g. int4) decodes to garbage on the client.
                ReadOnlyMemory<byte>? replaceValueBytes = replacedValue != null
                    ? GetValueByType(property, replacedValue, replaceColumn)
                    : null;

                row[replaceColumn.ColumnIndex] = replaceValueBytes;

                HandleSpecialColumnsIfNeeded(replace.DstColumnName, property, replacedValue, row);
            }
        }
    }
}
