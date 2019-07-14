using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Server.Documents.PeriodicBackup
{
    public class Retention : RavenTestBase
    {
        [NightlyBuildTheory]
        [InlineData(20, 5)]
        [InlineData(20, 20)]
        [InlineData(25, 10)]
        [InlineData(30, 3)]
        [InlineData(40, 20)]
        [InlineData(45, 1)]
        [InlineData(50, 50)]
        [InlineData(70, 13)]
        public async Task can_delete_backups_by_date(int backupAgeInSeconds, int numberOfBackupsToCreate)
        {
            using (var test = new SlowTests.Server.Documents.PeriodicBackup.Retention())
            {
                await test.can_delete_backups_by_date(backupAgeInSeconds, numberOfBackupsToCreate);
            }
        }

        [NightlyBuildTheory(Skip = "Requires Amazon AWS Credentials")]
        [InlineData(20, 5)]
        [InlineData(20, 20)]
        [InlineData(25, 10)]
        [InlineData(30, 3)]
        [InlineData(40, 20)]
        [InlineData(45, 1)]
        [InlineData(50, 50)]
        [InlineData(70, 13)]
        public async Task can_delete_backups_by_date_s3(int backupAgeInSeconds, int numberOfBackupsToCreate)
        {
            using (var test = new SlowTests.Server.Documents.PeriodicBackup.Retention())
            {
                await test.can_delete_backups_by_date_s3(backupAgeInSeconds, numberOfBackupsToCreate);
            }
        }
    }
}
