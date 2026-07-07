using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // Per-collection PK column row (synthetic `id` at position 1). Joined to
    // table_constraints on `pk_<TableName>` so PowerBI's PK probe sees exactly one row per
    // table. See PgSyntheticColumns for the `id` vs RQL `id()` naming.
    internal sealed class InformationSchemaKeyColumnUsageTable : PgVirtualTable
    {
        private const string PublicSchema = "public";
        private const int FirstOrdinalPosition = 1;

        public override string SchemaName => "information_schema";
        public override string TableName => "key_column_usage";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("constraint_catalog",            PgName.Default, PgFormat.Text),
            new("constraint_schema",             PgName.Default, PgFormat.Text),
            new("constraint_name",               PgName.Default, PgFormat.Text),
            new("table_catalog",                 PgName.Default, PgFormat.Text),
            new("table_schema",                  PgName.Default, PgFormat.Text),
            new("table_name",                    PgName.Default, PgFormat.Text),
            new("column_name",                   PgName.Default, PgFormat.Text),
            new("ordinal_position",              PgInt4.Default, PgFormat.Text),
            new("position_in_unique_constraint", PgInt4.Default, PgFormat.Text),
        };

        public override IEnumerable<object[]> EnumerateRows(VirtualQueryContext ctx)
        {
            if (ctx?.Database == null)
                yield break;

            var dbName = ctx.Database.Name;

            using (ctx.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in ctx.Database.DocumentsStorage.GetCollections(context))
                {
                    if (CollectionName.IsHiLoCollection(collection.Name))
                        continue;

                    yield return new object[]
                    {
                        dbName,                          // constraint_catalog
                        PublicSchema,                    // constraint_schema
                        $"pk_{collection.Name}",         // constraint_name
                        dbName,                          // table_catalog
                        PublicSchema,                    // table_schema
                        collection.Name,                 // table_name
                        PgSyntheticColumns.DocumentId,   // column_name
                        FirstOrdinalPosition,            // ordinal_position
                        null,                            // position_in_unique_constraint (only set for FKs)
                    };
                }
            }
        }
    }
}
