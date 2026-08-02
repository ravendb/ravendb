using System;
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
    // The last thing standing between SQLAlchemy and a reflected column list. Its get_columns()
    // reads identity-column metadata through a scalar subquery over pg_catalog.pg_sequence; the
    // table wasn't registered, so the whole SQL_COLS statement was rejected - format_type, the
    // pg_attrdef default lookup and the pg_description join all resolved, and none of it mattered.
    //
    // RavenDB has no sequences, so an empty pg_sequence is the correct answer. What makes
    // registration alone sufficient is that the subquery's own machinery - json_build_object() in
    // its projection, pg_get_serial_sequence() and the ::regclass casts in its WHERE - only runs
    // per row of the pg_sequence/pg_class join, and that join has no rows.
    public class PgCatalogPgSequenceTests : RavenTestBase
    {
        public PgCatalogPgSequenceTests(ITestOutputHelper output) : base(output)
        {
        }

        // SQLAlchemy 1.4.54's PGDialect.get_columns() SQL_COLS, verbatim, with the two %s
        // placeholders filled in the way it fills them for the server_version we report (13.3 -
        // so `a.attgenerated as generated` and the pg_sequence identity subquery, not their
        // pre-PG-10/12 NULL stand-ins) and the :table_oid bind parameter substituted.
        private const string SqlCols = """
                        SELECT a.attname,
                          pg_catalog.format_type(a.atttypid, a.atttypmod),
                          (
                            SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                            FROM pg_catalog.pg_attrdef d
                            WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum
                            AND a.atthasdef
                          ) AS DEFAULT,
                          a.attnotnull,
                          a.attrelid as table_oid,
                          pgd.description as comment,
                          a.attgenerated as generated,
                            (SELECT json_build_object(
                                'always', a.attidentity = 'a',
                                'start', s.seqstart,
                                'increment', s.seqincrement,
                                'minvalue', s.seqmin,
                                'maxvalue', s.seqmax,
                                'cache', s.seqcache,
                                'cycle', s.seqcycle)
                            FROM pg_catalog.pg_sequence s
                            JOIN pg_catalog.pg_class c on s.seqrelid = c."oid"
                            WHERE c.relkind = 'S'
                            AND a.attidentity != ''
                            AND s.seqrelid = pg_catalog.pg_get_serial_sequence(
                                a.attrelid::regclass::text, a.attname
                            )::regclass::oid
                            ) as identity_options
                        FROM pg_catalog.pg_attribute a
                        LEFT JOIN pg_catalog.pg_description pgd ON (
                            pgd.objoid = a.attrelid AND pgd.objsubid = a.attnum)
                        WHERE a.attrelid = {0}
                        AND a.attnum > 0 AND NOT a.attisdropped
                        ORDER BY a.attnum
                    """;

        // The whole statement, over a collection that has documents: it resolves, and the columns
        // it reports are the collection's columns. This is the query whose rejection left Superset
        // with "Unable to load columns for the selected table" (Zoho Desk #7031).
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Sqlalchemys_get_columns_statement_resolves_and_reports_the_collections_columns()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);
            var ordersOid = OidOf(ctx, "Orders");

            Assert.True(PgVirtualInterpreter.TryExecute(string.Format(SqlCols, ordersOid), ctx, out var table));

            Assert.Equal(
                new[] { "attname", "?column?", "default", "attnotnull", "table_oid", "comment", "generated", "identity_options" },
                ColumnNames(table));
            Assert.Equal(new[] { "id", "Company", "Freight", "json" }, ColumnValues(table, column: 0));
            Assert.Equal(new[] { "text", "text", "bigint", "json" }, ColumnValues(table, column: 1));

            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.False(table.Data[row].ColumnData.Span[2].HasValue);  // default - pg_attrdef is empty
                Assert.Equal("f", DecodeCell(table, row, column: 3));       // attnotnull
                Assert.Equal(ordersOid, DecodeCell(table, row, column: 4)); // table_oid
                Assert.False(table.Data[row].ColumnData.Span[5].HasValue);  // comment - pg_description is empty
                Assert.Equal(string.Empty, DecodeCell(table, row, column: 6)); // generated - a plain column

                // No rows in pg_sequence means the subquery yields SQL NULL - "this column is not
                // an identity column" - which is true of every RavenDB document field.
                Assert.False(table.Data[row].ColumnData.Span[7].HasValue);
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_sequence_is_registered_and_holds_no_rows()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select seqrelid, seqstart, seqincrement, seqmin, seqmax, seqcache, seqcycle from pg_catalog.pg_sequence",
                new VirtualQueryContext(), out var table));

            Assert.Equal(
                new[] { "seqrelid", "seqstart", "seqincrement", "seqmin", "seqmax", "seqcache", "seqcycle" },
                ColumnNames(table));
            Assert.Empty(table.Data);
        }

        // Why registering the table was enough, isolated from the correlation. json_build_object()
        // sits in the subquery's projection and pg_get_serial_sequence() in its WHERE, and both are
        // only reached per row of the pg_sequence JOIN pg_class join. pg_class is populated here -
        // it is pg_sequence's emptiness alone that empties the join, so neither function runs. And
        // they are genuinely absent, as the second half shows: if pg_sequence ever gained a row,
        // this would start failing on the missing function rather than inventing identity metadata.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_identity_subquerys_machinery_is_not_implemented_and_is_never_reached()
        {
            using var store = GetDocumentStore();
            StoreSampleDocuments(store);

            var ctx = await CtxFor(store);

            Assert.True(PgVirtualInterpreter.TryExecute(
                """
                SELECT json_build_object('start', s.seqstart, 'cycle', s.seqcycle)
                FROM pg_catalog.pg_sequence s
                JOIN pg_catalog.pg_class c on s.seqrelid = c."oid"
                WHERE c.relkind = 'S'
                AND s.seqrelid = pg_catalog.pg_get_serial_sequence('Orders'::regclass::text, 'id')::regclass::oid
                """, ctx, out var overEmptyPgSequence));
            Assert.Empty(overEmptyPgSequence.Data);

            // pg_class on its own is not empty - the join is empty because pg_sequence is.
            Assert.True(PgVirtualInterpreter.TryExecute("select oid from pg_class where relkind = 'r'", ctx, out var pgClass));
            Assert.NotEmpty(pgClass.Data);

            Assert.False(PgVirtualInterpreter.TryExecute(
                "select json_build_object('always', true)", ctx, out _));
            Assert.False(PgVirtualInterpreter.TryExecute(
                "select pg_catalog.pg_get_serial_sequence('Orders', 'id')", ctx, out _));
        }

        private static void StoreSampleDocuments(IDocumentStore store)
        {
            using var session = store.OpenSession();
            session.Store(new Order { Company = "companies/1", Freight = 42 }, "orders/1");
            session.SaveChanges();
        }

        private async Task<VirtualQueryContext> CtxFor(IDocumentStore store)
            => new() { Database = await Databases.GetDocumentDatabaseInstanceFor(store), Username = "root" };

        // The oid SQLAlchemy's get_table_oid() resolves before it runs SQL_COLS.
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

        private static IEnumerable<string> ColumnValues(PgTable table, int column)
        {
            var values = new List<string>(table.Data.Count);
            for (int row = 0; row < table.Data.Count; row++)
                values.Add(DecodeCell(table, row, column));
            return values;
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
