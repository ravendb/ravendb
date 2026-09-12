using System;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class PgSessionErrorHandlingTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Unexpected_exception_from_a_message_becomes_a_readable_pg_error()
        {
            var message = new ThrowingMessage(new InvalidQueryException("Field 'count()' is neither an aggregation operation nor part of the group by key"));

            var error = await Assert.ThrowsAsync<PgErrorException>(() => HandleAsync(message));

            Assert.Equal(PgErrorCodes.SyntaxErrorOrAccessRuleViolation, error.ErrorCode);
            Assert.Contains("count()", error.Message);
            Assert.IsType<InvalidQueryException>(error.InnerException);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Unexpected_non_query_exception_becomes_an_internal_error()
        {
            var message = new ThrowingMessage(new InvalidOperationException("boom"));

            var error = await Assert.ThrowsAsync<PgErrorException>(() => HandleAsync(message));

            Assert.Equal(PgErrorCodes.InternalError, error.ErrorCode);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Existing_pg_error_passes_through_unchanged()
        {
            var message = new ThrowingMessage(new PgErrorException(PgErrorCodes.FeatureNotSupported, "unsupported"));

            var error = await Assert.ThrowsAsync<PgErrorException>(() => HandleAsync(message));

            Assert.Equal(PgErrorCodes.FeatureNotSupported, error.ErrorCode);
            Assert.Null(error.InnerException);
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Fatal_protocol_violation_stays_fatal()
        {
            var message = new ThrowingMessage(new PgFatalException(PgErrorCodes.ProtocolViolation, "bad message"));

            await Assert.ThrowsAsync<PgFatalException>(() => HandleAsync(message));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Terminate_and_cancellation_are_not_reported_to_the_client()
        {
            await Assert.ThrowsAsync<PgTerminateReceivedException>(() => HandleAsync(new ThrowingMessage(new PgTerminateReceivedException())));
            await Assert.ThrowsAsync<OperationCanceledException>(() => HandleAsync(new ThrowingMessage(new OperationCanceledException())));
        }

        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Broken_connection_is_not_reported_to_the_client()
        {
            await Assert.ThrowsAsync<IOException>(() => HandleAsync(new ThrowingMessage(new IOException("peer closed"))));
        }

        // The client must be able to keep using the connection: an error on a Query is followed by
        // ReadyForQuery, which is what tells libpq the session is still usable.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public async Task Query_error_is_followed_by_ready_for_query()
        {
            using var messageBuilder = new MessageBuilder();
            using var transaction = NewTransaction();
            var stream = new MemoryStream();
            var writer = PipeWriter.Create(stream);

            await new Query().HandleError(
                new PgErrorException(PgErrorCodes.InternalError, "boom", new InvalidOperationException("boom")),
                transaction, messageBuilder, writer, CancellationToken.None);

            await writer.FlushAsync();

            var bytes = stream.ToArray();
            Assert.Equal((byte)MessageType.ErrorResponse, bytes[0]);
            Assert.Equal((byte)MessageType.ReadyForQuery, bytes[^6]);
        }

        private static async Task HandleAsync(Message message)
        {
            using var messageBuilder = new MessageBuilder();
            using var transaction = NewTransaction();
            var stream = new MemoryStream();

            await message.Handle(transaction, messageBuilder, PipeReader.Create(stream), PipeWriter.Create(stream), CancellationToken.None);
        }

        private static PgTransaction NewTransaction()
        {
            var session = new PgSession(client: null, serverCertificateHolder: null, identifier: 0, processId: 0, serverStore: null, token: default);
            return new PgTransaction(documentDatabase: null, messageReader: new MessageReader(), username: null, session: session);
        }

        private sealed class ThrowingMessage(Exception toThrow) : Message
        {
            protected override Task<int> InitMessage(MessageReader messageReader, PipeReader reader, int msgLen, CancellationToken token) => Task.FromResult(0);

            protected override Task HandleMessage(PgTransaction transaction, MessageBuilder messageBuilder, PipeWriter writer, CancellationToken token) => throw toThrow;
        }
    }
}
