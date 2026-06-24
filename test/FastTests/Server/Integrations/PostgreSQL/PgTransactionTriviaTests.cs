using Raven.Server.Integrations.PostgreSQL;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    // Pins the classifier that decides whether a leading statement in a multi-statement Parse
    // is safe to silently drop. The trivia allowlist is the wire-level contract for which
    // Npgsql / Microsoft Fabric / pgAdmin startup-probe shapes we tolerate. Anything not on
    // the list — i.e. anything that might be a real query — must NOT be classified as trivia.
    public sealed class PgTransactionTriviaTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SET DateStyle = ISO")]
        [InlineData("SET client_min_messages = notice")]
        [InlineData("set search_path to public")]
        [InlineData("SHOW server_version")]
        [InlineData("RESET ALL")]
        [InlineData("BEGIN")]
        [InlineData("begin transaction")]
        [InlineData("COMMIT")]
        [InlineData("ROLLBACK")]
        [InlineData("END")]
        [InlineData("START TRANSACTION")]
        [InlineData("SELECT version()")]
        [InlineData("select version ( )")]
        [InlineData("SELECT current_setting('timezone')")]
        [InlineData("SELECT pg_catalog.set_config('application_name', 'x', false)")]
        [InlineData("   ")]
        [InlineData("")]
        public void Trivia_shapes_are_classified_as_trivia(string stmt)
        {
            Assert.True(PgTransaction.IsTriviaStatement(stmt));
        }

        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("SELECT * FROM \"Orders\"")]
        [InlineData("SELECT 1")]
        [InlineData("SELECT typname FROM pg_type")]
        [InlineData("UPDATE \"Orders\" SET \"Freight\" = 0")]
        [InlineData("DELETE FROM \"Orders\"")]
        [InlineData("INSERT INTO \"Orders\" VALUES (1)")]
        [InlineData("WITH x AS (SELECT 1) SELECT * FROM x")]
        public void Real_queries_are_NOT_classified_as_trivia(string stmt)
        {
            Assert.False(PgTransaction.IsTriviaStatement(stmt));
        }

        // Word-boundary guard: an identifier that starts with a keyword prefix must NOT be
        // classified as trivia. e.g. a column named `setting` shouldn't match the `SET` keyword.
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void Keyword_prefix_inside_identifier_does_not_classify_as_trivia()
        {
            Assert.False(PgTransaction.IsTriviaStatement("SELECT settings FROM \"Configs\""));
            Assert.False(PgTransaction.IsTriviaStatement("setteler"));
            Assert.False(PgTransaction.IsTriviaStatement("commitments"));
        }
    }
}
