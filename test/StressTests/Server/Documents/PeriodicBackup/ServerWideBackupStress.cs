using System.Threading.Tasks;
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
            // ReSharper disable once UseAwaitUsing
            using var stress = new SlowTests.Issues.RavenDB_14292(Output);
            await stress.ServerWideBackupShouldBackupIdleDatabase(rounds);
        }
    }
}
