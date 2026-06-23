using Raven.Server.Integrations.PostgreSQL;
using Raven.Server.Integrations.PostgreSQL.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class ProtocolCommandQueryTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        // Only `DEALLOCATE "<name>"` is parsed; ALL / unquoted / bare forms hit a branch that throws
        // before touching the session, so a null session is fine here.
        [RavenTheory(RavenTestCategory.PostgreSql)]
        [InlineData("DEALLOCATE ALL")]
        [InlineData("DEALLOCATE foo")]
        [InlineData("DEALLOCATE")]
        public void Deallocate_unsupportedForm_throwsNonFatalPgError(string sql)
        {
            var ex = Assert.Throws<PgErrorException>(
                () => ProtocolCommandQuery.TryParse(sql, parametersDataTypes: null, session: null, out _));

            Assert.Equal(PgErrorCodes.FeatureNotSupported, ex.ErrorCode);
        }
    }
}
