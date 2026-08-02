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
    // pg_am lists index access methods - btree, hash, gist and so on. RavenDB exposes none of
    // them: a collection has no indexes in the PG sense, so there is no access method to name.
    //
    // SQLAlchemy's get_indexes() LEFT-JOINs it to label each index with its method. Unregistered,
    // that arm made JoinExecutor.TryResolveSource return false and rejected IDX_SQL outright; empty
    // instead leaves am.amname NULL, which is what a LEFT JOIN with nothing to match yields.
    public class PgCatalogPgAmTests : RavenTestBase
    {
        public PgCatalogPgAmTests(ITestOutputHelper output) : base(output)
        {
        }

        // The access-method arm of IDX_SQL, spelled the way SQLAlchemy 1.4.54 spells it, over the
        // populated pg_class it joins from. The full IDX_SQL needs pg_class.reloptions as well,
        // which lands in the next commit.
        private const string AccessMethodJoin = """
                  SELECT
                      i.relname as relname, am.amname
                  FROM
                      pg_class i
                            left outer join
                                pg_am am
                                on i.relam = am.oid
                  WHERE
                      i.relkind IN ('r', 'v', 'f', 'm', 'p')
                  ORDER BY
                      i.relname
                """;

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task The_access_method_join_resolves_and_names_no_access_method()
        {
            using var store = GetDocumentStore();
            var ctx = await PopulatedCatalog(store);

            Assert.True(PgVirtualInterpreter.TryExecute(AccessMethodJoin, ctx, out var table));

            Assert.Equal(new[] { "relname", "amname" }, ColumnNames(table));

            // pg_class has rows, so the LEFT JOIN preserves them - it is pg_am that contributes
            // nothing, leaving amname NULL on every one.
            Assert.NotEmpty(table.Data);
            for (int row = 0; row < table.Data.Count; row++)
            {
                Assert.True(table.Data[row].ColumnData.Span[0].HasValue);
                Assert.False(table.Data[row].ColumnData.Span[1].HasValue);
            }
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Pg_am_is_registered_and_holds_no_rows()
        {
            Assert.True(PgVirtualInterpreter.TryExecute(
                "select oid, amname from pg_catalog.pg_am", new VirtualQueryContext(), out var table));

            Assert.Equal(new[] { "oid", "amname" }, ColumnNames(table));
            Assert.Empty(table.Data);
        }

        private async Task<VirtualQueryContext> PopulatedCatalog(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Company = "companies/1", Freight = 42 }, "orders/1");
                session.SaveChanges();
            }

            return new VirtualQueryContext
            {
                Database = await Databases.GetDocumentDatabaseInstanceFor(store),
                Username = "root"
            };
        }

        private static IEnumerable<string> ColumnNames(PgTable table)
        {
            var names = new List<string>(table.Columns.Count);
            foreach (var column in table.Columns)
                names.Add(column.Name);
            return names;
        }

        private sealed class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public long Freight { get; set; }
        }
    }
}
