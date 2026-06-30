using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // information_schema.columns: per-column metadata PowerBI and pgAdmin read to learn a collection's
    // column shape before the real SELECT.
    //
    // The reported columns MUST match what RqlQuery emits in its RowDescription (same count, order,
    // names, and types) or PowerBI raises DataSource.Changed. Hence: user columns in document insertion
    // order (GetPropertyNames, not GetPropertyByIndex), bracketed by the synthetic id/json columns,
    // with types mirroring RqlQuery's mapping (see MapDataType).
    //
    // table_catalog = ctx.Database.Name; table_schema = "public" (we don't model multiple schemas).
    internal sealed class InformationSchemaColumnsTable : PgVirtualTable
    {
        private const string TableNamePredicate = "table_name";
        private const string Yes = "YES";

        public override string SchemaName => "information_schema";
        public override string TableName => "columns";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("table_catalog",    PgName.Default,    PgFormat.Text),
            new("table_schema",     PgName.Default,    PgFormat.Text),
            new("table_name",       PgName.Default,    PgFormat.Text),
            new("column_name",      PgName.Default,    PgFormat.Text),
            new("ordinal_position", PgInt4.Default,    PgFormat.Text),
            new("is_nullable",      PgVarchar.Default, PgFormat.Text),
            new("data_type",        PgVarchar.Default, PgFormat.Text),
        };

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            if (ctx?.Database == null)
                yield break;

            if (ctx.Predicates == null ||
                ctx.Predicates.TryGetValue(TableNamePredicate, out var rawTable) == false ||
                rawTable is not string collection ||
                string.IsNullOrWhiteSpace(collection))
                yield break;

            using (ctx.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                BlittableJsonReaderObject sample = null;
                foreach (var doc in ctx.Database.DocumentsStorage.GetDocumentsFrom(context, collection, etag: 0, start: 0, take: 1))
                {
                    sample = doc.Data;
                    break;
                }

                if (sample == null)
                    yield break;

                var dbName = ctx.Database.Name;
                int ordinal = 1;

                // 1) Synthetic id column (PG-facing name; see PgSyntheticColumns).
                yield return new object[]
                {
                    dbName, "public", collection,
                    PgSyntheticColumns.DocumentId,
                    ordinal++, Yes, "text"
                };

                // 2) User columns in document insertion order (same order RqlQuery emits).
                var prop = default(BlittableJsonReaderObject.PropertyDetails);
                foreach (var name in sample.GetPropertyNames())
                {
                    if (string.IsNullOrEmpty(name))
                        continue;
                    // Skip RavenDB system fields (@metadata, etc.); RqlQuery skips them too.
                    if (name.StartsWith('@'))
                        continue;

                    var propIdx = sample.GetPropertyIndex(name);
                    if (propIdx == -1)
                        continue;
                    sample.GetPropertyByIndex(propIdx, ref prop);

                    var dataType = MapDataType(prop.Token, prop.Value);
                    yield return new object[] { dbName, "public", collection, name, ordinal++, Yes, dataType };
                }

                // 3) json - the metadata blob column RqlQuery appends last (PgJson).
                yield return new object[]
                {
                    dbName, "public", collection,
                    PgSyntheticColumns.Json,
                    ordinal++, Yes, "json"
                };
            }
        }

        // Mirrors RqlQuery's BlittableJsonToken-to-PgType mapping so data_type here matches RqlQuery's
        // RowDescription (see the class doc on why that must hold). For String/CompressedString, peek
        // at the value like RqlQuery does: datetime-shaped strings map to timestamp, not text.
        private static string MapDataType(BlittableJsonToken token, object value)
        {
            var bjt = token & BlittableJsonToken.TypesMask;

            if (bjt is BlittableJsonToken.String or BlittableJsonToken.CompressedString)
            {
                var processedString = bjt == BlittableJsonToken.CompressedString
                    ? (string)(LazyCompressedStringValue)value
                    : (string)(LazyStringValue)value;

                if (processedString != null
                    && TypeConverter.TryConvertStringValue(processedString, out var parsed))
                {
                    return parsed switch
                    {
                        // RqlQuery: PgTimestamp / PgTimestampTz / PgInterval
                        System.DateTime dt   => dt.Kind == System.DateTimeKind.Utc
                                                    ? "timestamp with time zone"
                                                    : "timestamp without time zone",
                        System.DateTimeOffset => "timestamp with time zone",
                        System.TimeSpan       => "interval",
                        _                     => "text"
                    };
                }

                return "text";   // RqlQuery: PgText
            }

            return bjt switch
            {
                BlittableJsonToken.Integer           => "bigint",            // RqlQuery: PgInt8
                BlittableJsonToken.LazyNumber        => "double precision",  // RqlQuery: PgFloat8
                BlittableJsonToken.Boolean           => "boolean",           // RqlQuery: PgBool
                BlittableJsonToken.StartObject       => "json",              // RqlQuery: PgJson
                BlittableJsonToken.StartArray        => "json",              // RqlQuery: PgJson
                BlittableJsonToken.EmbeddedBlittable => "json",              // RqlQuery: PgJson
                BlittableJsonToken.Null              => "json",              // RqlQuery: PgJson
                _                                    => "json",              // unknown -> PgJson
            };
        }
    }
}
