using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.VirtualCatalog.Tables
{
    // Exposes one PRIMARY KEY row per Raven collection — synthetic `id` column. PowerBI's
    // DirectQuery joins this with key_column_usage to establish per-row identity.
    // PG-facing name is `id` (not RQL's `id()`) — see PgSyntheticColumns for the rationale.
    internal sealed class InformationSchemaTableConstraintsTable : PgVirtualTable
    {
        private const string PublicSchema = "public";
        private const string PrimaryKeyType = "PRIMARY KEY";
        private const string No = "NO";
        private const string Yes = "YES";

        public override string SchemaName => "information_schema";
        public override string TableName => "table_constraints";

        public override IReadOnlyList<PgVirtualColumn> Columns { get; } = new PgVirtualColumn[]
        {
            new("constraint_catalog", PgName.Default,    PgFormat.Text),
            new("constraint_schema",  PgName.Default,    PgFormat.Text),
            new("constraint_name",    PgName.Default,    PgFormat.Text),
            new("table_catalog",      PgName.Default,    PgFormat.Text),
            new("table_schema",       PgName.Default,    PgFormat.Text),
            new("table_name",         PgName.Default,    PgFormat.Text),
            new("constraint_type",    PgVarchar.Default, PgFormat.Text),
            new("is_deferrable",      PgVarchar.Default, PgFormat.Text),
            new("initially_deferred", PgVarchar.Default, PgFormat.Text),
            new("enforced",           PgVarchar.Default, PgFormat.Text),
            new("nulls_distinct",     PgVarchar.Default, PgFormat.Text),
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

                    // Synthetic constraint name `pk_<TableName>` joins to the matching row in
                    // key_column_usage. Every Raven collection has exactly one PK (the doc id),
                    // so no UNIQUE-constraint rows are emitted.
                    yield return new object[]
                    {
                        dbName,                       // constraint_catalog
                        PublicSchema,                 // constraint_schema
                        $"pk_{collection.Name}",      // constraint_name
                        dbName,                       // table_catalog
                        PublicSchema,                 // table_schema
                        collection.Name,              // table_name
                        PrimaryKeyType,               // constraint_type
                        No,                           // is_deferrable
                        No,                           // initially_deferred
                        Yes,                          // enforced
                        Yes,                          // nulls_distinct
                    };
                }
            }
        }
    }
}
