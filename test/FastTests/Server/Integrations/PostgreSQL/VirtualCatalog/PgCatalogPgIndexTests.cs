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
    // Superset won't save a dataset until get_pk_constraint() answers, and its PK_SQL reads
    // pg_catalog.pg_index. The table wasn't registered at all, so JoinExecutor.TryResolveSource
    // returned false and the whole statement was rejected - not the same thing as a registered
    // table with no rows, which answers cleanly with an empty rowset.
    //
    // RavenDB has no indexes on collections in the PG sense, so empty is the correct answer.
    public class PgCatalogPgIndexTests : RavenTestBase
    {
        public PgCatalogPgIndexTests(ITestOutputHelper output) : base(output)
        {
        }

        // SQLAlchemy 1.4.54's PGDialect.get_pk_constraint() PK_SQL, verbatim, taking the
        // server_version_info >= (8, 4) branch (we report 13.3) and with the :table_oid bind
        // parameter substituted.
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

        // The statement whose rejection stopped Superset from saving a dataset. It has to resolve
        // against a populated catalog - a real collection oid, a pg_attribute with rows for it -
        // and report no primary-key columns, because RavenDB collections have none.
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

        // Why registering the table was enough. PK_SQL's inner SELECT projects unnest() and
        // generate_subscripts(), neither of which we implement - the second half of this test
        // shows they genuinely aren't there. The subquery still resolves because its only FROM
        // source is always-empty, so TryDeriveEmptySubqueryColumns takes the aliased output
        // columns straight off the target list and never executes the body.
        //
        // pg_attribute on the outer side is populated, so it is pg_index's emptiness alone that
        // empties the join. If pg_index ever gained a row this would start failing on the missing
        // functions rather than inventing key columns.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_set_returning_functions_in_the_subquery_are_not_implemented_and_are_never_reached()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            // The subquery on its own, standing in a FROM: derived to (attnum, ord), zero rows.
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

            // pg_attribute is not empty - the join is empty because pg_index is.
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
