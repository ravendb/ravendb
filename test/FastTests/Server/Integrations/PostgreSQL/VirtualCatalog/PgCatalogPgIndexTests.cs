using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.VirtualCatalog;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL.VirtualCatalog
{
    public class PgCatalogPgIndexTests : RavenTestBase
    {
        public PgCatalogPgIndexTests(ITestOutputHelper output) : base(output)
        {
        }

        // SQLAlchemy 1.4.54's PGDialect.get_pk_constraint() PK_SQL, verbatim, with :table_oid substituted.
        private const string PkSql = """
                        SELECT a.attname
                        FROM pg_attribute a JOIN (
                            SELECT unnest(ix.indkey) attnum,
                                   generate_subscripts(ix.indkey, 1) ord
                            FROM pg_index ix
                            WHERE ix.indrelid = {0} AND ix.indisprimary
                            ) k ON a.attnum=k.attnum
                        WHERE a.attrelid = {0}
                        ORDER BY k.ord
                    """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_pk_constraint_statement_resolves_and_reports_no_key_columns()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(PkSql, ordersOid), ctx, out var table));

            Assert.Equal(new[] { "attname" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_index_is_registered_and_holds_no_rows()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select indrelid, indexrelid, indisunique, indisprimary, indexprs, indpred, " +
                "indkey, indoption, indnkeyatts from pg_catalog.pg_index",
                new VirtualQueryContext(), out var table));

            Assert.Equal(
                new[] { "indrelid", "indexrelid", "indisunique", "indisprimary", "indexprs", "indpred", "indkey", "indoption", "indnkeyatts" },
                ColumnNames(table));
            Assert.Empty(table.Data);
        }

        // The subquery's only FROM source is always-empty, so its columns are derived without executing the body.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_set_returning_functions_in_the_subquery_are_not_implemented_and_are_never_reached()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"""
                 SELECT k.attnum, k.ord
                 FROM (
                     SELECT unnest(ix.indkey) attnum,
                            generate_subscripts(ix.indkey, 1) ord
                     FROM pg_index ix
                     WHERE ix.indrelid = {ordersOid} AND ix.indisprimary
                     ) k
                 """, ctx, out var subquery));
            Assert.Equal(new[] { "attnum", "ord" }, ColumnNames(subquery));
            Assert.Empty(subquery.Data);

            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select attname from pg_attribute where attrelid = {ordersOid}", ctx, out var attributes));
            Assert.NotEmpty(attributes.Data);

            Assert.False(PgVirtualInterpreter.TryExecute("select unnest('{1,2}')", ctx, out _));
            Assert.False(PgVirtualInterpreter.TryExecute("select generate_subscripts('{1,2}', 1)", ctx, out _));
        }

        private static void StoreSampleDocuments(IDocumentStore store)
        {
            using var session = store.OpenSession();
            session.Store(new Order { Company = "companies/1", Freight = 42 }, "orders/1");
            session.SaveChanges();
        }

        private async Task<VirtualQueryContext> CtxFor(IDocumentStore store)
            => new() { Database = await Databases.GetDocumentDatabaseInstanceFor(store), Username = "root" };

        // The oid SQLAlchemy's get_table_oid() resolves before it runs PK_SQL.
        private static string OidOf(VirtualQueryContext ctx, string collection)
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                $"select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace " +
                $"where n.nspname = 'public' and c.relname = '{collection}' and c.relkind in ('r','v','m','f','p')",
                ctx, out var table));
            Assert.Single(table.Data);
            return DecodeCell(table, row: 0, column: 0);
        }

        private static IEnumerable<string> ColumnNames(PgTable table)
        {
            var names = new List<string>(table.Columns.Count);
            foreach (var column in table.Columns)
                names.Add(column.Name);
            return names;
        }

        private static string DecodeCell(PgTable table, int row, int column)
        {
            var cell = table.Data[row].ColumnData.Span[column];
            Assert.True(cell.HasValue);
            return Encoding.UTF8.GetString(cell.Value.Span);
        }

        private sealed class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public long Freight { get; set; }
        }
    }
}
