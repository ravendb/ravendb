using System.Collections.Generic;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // information_schema.columns: per-column metadata PowerBI and pgAdmin read to learn a collection's
    // column shape before the real SELECT.
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
                var dbName = ctx.Database.Name;
                int ordinal = 1;

                foreach (var column in CollectionCatalog.Columns(ctx.Database, context, collection))
                {
                    yield return new object[]
                    {
                        dbName, "public", collection,
                        column.Name,
                        ordinal++, Yes, CollectionCatalog.DataTypeName(column.PgType)
                    };
                }
            }
        }
    }
}
