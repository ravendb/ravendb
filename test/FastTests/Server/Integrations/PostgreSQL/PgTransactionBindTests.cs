using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class PgTransactionBindTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Bind_to_unknown_prepared_statement_is_recoverable_not_fatal()
        {
            var session = new PgSession(client: null, serverCertificateHolder: null, identifier: 0, processId: 0, serverStore: null, token: default);
            using var transaction = new PgTransaction(documentDatabase: null, messageReader: new MessageReader(), username: null, session: session);

            // NamedStatements is empty, so this name resolves to nothing.
            var error = Assert.Throws<PgErrorException>(() => transaction.Bind(
                parameters: Array.Empty<byte[]>(),
                parameterFormatCodes: Array.Empty<short>(),
                resultColumnFormatCodes: Array.Empty<short>(),
                statementName: "statement_the_server_never_prepared"));

            Assert.Equal(PgErrorCodes.InvalidSqlStatementName, error.ErrorCode);
        }

        // Re-binding the same (prepared) statement instance with a parameter must not throw: PgQuery.Bind
        // re-populates the once-allocated Parameters dictionary, so without a Clear the 2nd bind throws
        // ArgumentException on the duplicate "1" key and the connection dies (the Npgsql auto-prepare path).
        // The pre-existing reuse test used a parameterless query, so the bind loop never ran.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Rebinding_a_parameterized_statement_does_not_throw()
        {
            using var query = new RqlQuery("from Orders", new[] { PgTypeOIDs.Text }, documentDatabase: null);
            var parameters = new List<byte[]> { Encoding.UTF8.GetBytes("hello") };
            var textFormat = new short[] { 0 };

            query.Bind(parameters, textFormat, Array.Empty<short>());
            query.Bind(parameters, textFormat, Array.Empty<short>()); // 2nd bind must not throw

            Assert.Single(query.Parameters);
        }

        // A named/prepared statement is cached in Session.NamedStatements and only borrowed by the
        // transaction. Sync()/Close() must NOT dispose it (it stays reusable for the next Bind/Execute);
        // session teardown (Dispose) drains and disposes it so it doesn't leak.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Named_statement_survives_sync_and_is_disposed_on_teardown()
        {
            var session = new PgSession(client: null, serverCertificateHolder: null, identifier: 0, processId: 0, serverStore: null, token: default);
            var transaction = new PgTransaction(documentDatabase: null, messageReader: new MessageReader(), username: null, session: session);

            var named = new TrackingPgQuery();
            transaction._currentQuery = named;       // the just-Parsed statement
            transaction.RegisterNamedStatement("S");  // cache it under a name

            transaction.Sync();
            Assert.False(named.Disposed);             // borrowed - not disposed on reset
            Assert.True(session.NamedStatements.ContainsKey("S"));

            transaction.Dispose();
            Assert.True(named.Disposed);              // drained on teardown
            Assert.Empty(session.NamedStatements);
        }

        // An unnamed (transient) statement is owned by the transaction and IS disposed on reset.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Unnamed_statement_is_disposed_on_sync()
        {
            var session = new PgSession(client: null, serverCertificateHolder: null, identifier: 0, processId: 0, serverStore: null, token: default);
            using var transaction = new PgTransaction(documentDatabase: null, messageReader: new MessageReader(), username: null, session: session);

            var unnamed = new TrackingPgQuery();
            transaction._currentQuery = unnamed;      // never registered → owned transient

            transaction.Sync();
            Assert.True(unnamed.Disposed);
        }

        private sealed class TrackingPgQuery() : PgQuery("from Tracking", Array.Empty<int>())
        {
            public bool Disposed { get; private set; }
            public override Task<ICollection<PgColumn>> Init() => Task.FromResult<ICollection<PgColumn>>(Array.Empty<PgColumn>());
            public override Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token) => Task.CompletedTask;
            public override void Dispose() => Disposed = true;
        }
    }
}
