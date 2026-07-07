using Raven.Server.Integrations.PostgreSQL;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class RqlIdentifierTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        // Identifier-shaped names are safe to splice unquoted - including non-ASCII letters (char.IsLetter
        // is Unicode-aware; they're also valid JS identifiers).
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("Company")]
        [InlineData("_id")]
        [InlineData("a1")]
        [InlineData("FirstName2")]
        [InlineData("café")]
        [InlineData("Имя")]
        [InlineData("名前")]
        public void Identifier_shaped_names_are_safe(string name) => Assert.True(RqlIdentifier.IsSafe(name));

        // Anything that would need quoting must be rejected so the caller falls through instead of
        // emitting broken output on the unquoted paths.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("Field With Space")]
        [InlineData("a'b")]
        [InlineData("a\"b")]
        [InlineData("a.b")]
        [InlineData("a-b")]
        [InlineData("1abc")]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData(null)]
        public void Names_needing_quoting_are_rejected(string name) => Assert.False(RqlIdentifier.IsSafe(name));
    }
}
