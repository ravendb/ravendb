using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    // Superset's simplest aggregate chart emits ORDER BY over a COUNT(*). The shape is unsupported,
    // but it has to come back as an error the client can read instead of killing the session.
    public sealed class PgGroupByOrderByAggregateTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        // ORDER BY count(*): the translator emits `order by 'count()'`, which the query engine rejects
        // at execution time with an InvalidQueryException.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT \"Company\", count(*) FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY count(*) DESC LIMIT 10")]
        // ORDER BY on the aggregate's alias: the translator rejects it up front, so it never gets built.
        [InlineData("SELECT \"Company\" AS \"Company\", COUNT(*) AS count FROM public.\"Orders\" GROUP BY \"Company\" ORDER BY count DESC LIMIT 10000")]
        public async Task OrderBy_over_an_aggregate_returns_a_pg_error(string sql)
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order { Company = "companies/1" }, "Orders/1");
                await session.SaveChangesAsync();
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            using var messageBuilder = new MessageBuilder();
            var pgSession = new PgSession(client: null, serverCertificateHolder: null, identifier: 0, processId: 0, serverStore: null, token: default);
            using var transaction = new PgTransaction(database, new MessageReader(), username: null, pgSession);
            var stream = new MemoryStream();

            var error = await Assert.ThrowsAsync<PgErrorException>(() => new Query { QueryString = sql }.Handle(
                transaction, messageBuilder, PipeReader.Create(stream), PipeWriter.Create(stream), CancellationToken.None));

            Assert.False(string.IsNullOrWhiteSpace(error.Message));
        }

        private sealed class Order
        {
            public string Company { get; set; }
        }
    }
}
