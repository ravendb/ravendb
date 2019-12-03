using System.Threading.Tasks;
using Raven.Server.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class ServerWideBackupStress : NoDisposalNoOutputNeeded
    {
        public ServerWideBackupStress(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(2)]
        [InlineData(5)]
        public async Task ServerWideBackupShouldBackupIdleDatabaseStress(int rounds)
        {
            await using var stress = new SlowTests.Issues.RavenDB_14292(Output);
            try
            {
                DatabasesLandlord.SkipShouldContinueDisposeCheck = true;
                await stress.ServerWideBackupShouldBackupIdleDatabase(rounds);
            }
            finally
            {
                DatabasesLandlord.SkipShouldContinueDisposeCheck = false;
            }
        }
    }
}
