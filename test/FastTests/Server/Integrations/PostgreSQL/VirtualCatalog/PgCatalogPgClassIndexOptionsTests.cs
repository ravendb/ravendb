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
    public class PgCatalogPgClassIndexOptionsTests : RavenTestBase
    {
        public PgCatalogPgClassIndexOptionsTests(ITestOutputHelper output) : base(output)
        {
        }

        // SQLAlchemy 1.4.54's PGDialect.get_indexes() IDX_SQL, verbatim, with :table_oid substituted.
        private const string IdxSql = """
                      SELECT
                          i.relname as relname,
                          ix.indisunique, ix.indexprs,
                          a.attname, a.attnum, c.conrelid, ix.indkey::varchar,
                          ix.indoption::varchar, i.reloptions, am.amname,
                          pg_get_expr(ix.indpred, ix.indrelid),
                          ix.indnkeyatts as indnkeyatts
                      FROM
                          pg_class t
                                join pg_index ix on t.oid = ix.indrelid
                                join pg_class i on i.oid = ix.indexrelid
                                left outer join
                                    pg_attribute a
                                    on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)
                                left outer join
                                    pg_constraint c
                                    on (ix.indrelid = c.conrelid and
                                        ix.indexrelid = c.conindid and
                                        c.contype in ('p', 'u', 'x'))
                                left outer join
                                    pg_am am
                                    on i.relam = am.oid
                      WHERE
                          t.relkind IN ('r', 'v', 'f', 'm', 'p')
                          and t.oid = {0}
                          and ix.indisprimary = 'f'
                      ORDER BY
                          t.relname,
                          i.relname
                    """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_indexes_statement_resolves_and_reports_no_indexes()
        {
            using var store = GetDocumentStore();
            var (ctx, oid) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(IdxSql, oid), ctx, out var table));

            // SQLAlchemy unpacks positionally; unaliased expressions we name "?column?" are read by position.
            Assert.Equal(
                new[]
                {
                    "relname", "indisunique", "indexprs", "attname", "attnum", "conrelid",
                    "?column?", "?column?", "reloptions", "amname", "?column?", "indnkeyatts"
                },
                ColumnNames(table));
            Assert.Empty(table.Data);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Pg_class_declares_reloptions_and_relam_as_null_on_every_row()
        {
            using var store = GetDocumentStore();
            var (ctx, _) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select relname, reloptions, relam from pg_catalog.pg_class", ctx, out var table));

            Assert.Equal(new[] { "relname", "reloptions", "relam" }, ColumnNames(table));
            Assert.NotEmpty(table.Data);

            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.True(table.Data[row].ColumnData.Span[0].HasValue);
                Assert.False(table.Data[row].ColumnData.Span[1].HasValue);
                Assert.False(table.Data[row].ColumnData.Span[2].HasValue);
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_existing_pg_class_columns_are_unchanged()
        {
            using var store = GetDocumentStore();
            var (ctx, oid) = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid, relname, relnamespace, relkind, typrelid from pg_catalog.pg_class " +
                $"where oid = {oid}", ctx, out var table));

            Assert.Equal(new[] { "oid", "relname", "relnamespace", "relkind", "typrelid" }, ColumnNames(table));
            Assert.Single(table.Data);
            Assert.Equal("Orders", DecodeCell(table, row: 0, column: 1));
            Assert.Equal("2200", DecodeCell(table, row: 0, column: 2));
            Assert.Equal("r", DecodeCell(table, row: 0, column: 3));
        }

        private async Task<(VirtualQueryContext Ctx, string Oid)> PopulatedCatalog(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Company = "companies/1", Freight = 42 }, "orders/1");
                session.SaveChanges();
            }

            var ctx = new VirtualQueryContext
            {
                Database = await Databases.GetDocumentDatabaseInstanceFor(store),
                Username = "root"
            };
            return (ctx, OidOf(ctx, "Orders"));
        }

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
