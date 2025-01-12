using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Documents.ETL.SQL;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Documents.ETL
{
    internal class SqlEtlStressTests : RavenTestBase
    {
        public SqlEtlStressTests(ITestOutputHelper output) : base(output)
        {
        }

        [RequiresMsSqlRetryTheory(delayBetweenRetriesMs: 1000)]
        [InlineData(RavenDatabaseMode.Sharded)]
        public async Task Should_error_if_attachment_doesnt_exist(RavenDatabaseMode databaseMode)
        {
            await using (var test = new SqlEtlTests(Output))
            {
                await test.Should_error_if_attachment_doesnt_exist(databaseMode);
            }
        }
    }
}
