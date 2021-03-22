/*
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class Retention : NoDisposalNoOutputNeeded
    {
        public Retention(ITestOutputHelper output) : base(output)
        {
        }

        [NightlyBuildTheory]
        [InlineData(20, 5, false)]
        [InlineData(20, 20, false)]
        [InlineData(25, 10, false)]
        [InlineData(30, 3, false)]
        [InlineData(40, 20, false)]
        [InlineData(45, 1, false)]
        [InlineData(50, 50, false)]
        [InlineData(70, 13, false)]
        [InlineData(20, 5, true)]
        [InlineData(20, 20, true)]
        [InlineData(25, 10, true)]
        [InlineData(30, 3, true)]
        [InlineData(40, 20, true)]
        [InlineData(45, 1, true)]
        [InlineData(50, 50, true)]
        [InlineData(70, 13, true)]
        [InlineData(7, 3, false, "\\E\\G\\O\\R\\..\\..\\..\\..")]
        [InlineData(7, 3, false, "\\E/G\\../..\\O\\R/..\\..")]
        public async Task can_delete_backups_by_date(int backupAgeInSeconds, int numberOfBackupsToCreate, bool checkIncremental, string suffix = null)
        {
            using (var test = new SlowTests.Server.Documents.PeriodicBackup.Retention(Output))
            {
                await test.can_delete_backups_by_date(backupAgeInSeconds, numberOfBackupsToCreate, checkIncremental, suffix);
            }
        }

        [NightlyBuildTheory(Skip = "Requires Amazon AWS Credentials")]
        [InlineData(20, 5, false)]
        [InlineData(20, 20, false)]
        [InlineData(25, 10, false)]
        [InlineData(30, 3, false)]
        [InlineData(40, 20, false)]
        [InlineData(45, 1, false)]
        [InlineData(50, 50, false)]
        [InlineData(70, 13, false)]
        [InlineData(20, 5, true)]
        [InlineData(20, 20, true)]
        [InlineData(25, 10, true)]
        [InlineData(30, 3, true)]
        [InlineData(40, 20, true)]
        [InlineData(45, 1, true)]
        [InlineData(50, 50, true)]
        [InlineData(70, 13, true)]
        public async Task can_delete_backups_by_date_s3(int backupAgeInSeconds, int numberOfBackupsToCreate, bool checkIncremental)
        {
            using (var test = new SlowTests.Server.Documents.PeriodicBackup.Retention(Output))
            {
                await test.can_delete_backups_by_date_s3(backupAgeInSeconds, numberOfBackupsToCreate, checkIncremental);
            }
        }
    }
}
*/
