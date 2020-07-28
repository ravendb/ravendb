// -----------------------------------------------------------------------
//  <copyright file="PeriodicBackupTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Config.Categories;
using Xunit;
using Xunit.Abstractions;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;

namespace FastTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTests : RavenTestBase
    {
        public PeriodicBackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanSetupPeriodicBackupWithVeryLargePeriods()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    FullBackupFrequency = "* */1 * * *",
                    IncrementalBackupFrequency = "* */2 * * *"
                };

                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var periodicBackupRunner = (await GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                var backups = periodicBackupRunner.PeriodicBackups;
                Assert.Equal("* */1 * * *", backups.First().Configuration.FullBackupFrequency);
                Assert.Equal("* */2 * * *", backups.First().Configuration.IncrementalBackupFrequency);
            }
        }

        [Fact]
        public void CanSetupConfiguration()
        {
            var allDestinations = new HashSet<string>(BackupConfiguration._allDestinations);
            allDestinations.Remove(PeriodicBackupConfiguration.BackupDestination.None.ToString());

            var backupConfiguration = new BackupConfiguration();
            foreach (var destination in allDestinations)
            {
                backupConfiguration.AssertDestinationAllowed(destination);
            }

            backupConfiguration.AllowedDestinations = allDestinations.ToArray();
            foreach (var destination in allDestinations)
            {
                backupConfiguration.AssertDestinationAllowed(destination);
            }

            backupConfiguration.AllowedDestinations = new[] {"Amazonglacier", "Amazons3", "googleCloud"};
            backupConfiguration.ValidateAllowedDestinations();
            backupConfiguration.AssertDestinationAllowed("AmazonGlacier");
            backupConfiguration.AssertDestinationAllowed("AmazonS3");
            backupConfiguration.AssertDestinationAllowed("GoogleCloud");
            var exception = Assert.Throws<ArgumentException>(() => backupConfiguration.AssertDestinationAllowed("Google Cloud"));
            Assert.StartsWith("The selected backup destination 'Google Cloud' is not allowed in this RavenDB server", exception.Message);

            backupConfiguration.AllowedDestinations = new[] { "None" };
            foreach (var destination in allDestinations)
            {
                AssertNoneAllowedDestinations(destination);
            }

            backupConfiguration.AllowedDestinations = new[] { "nOnE" };
            foreach (var destination in allDestinations)
            {
                AssertNoneAllowedDestinations(destination);
            }

            void AssertNoneAllowedDestinations(string destination)
            {
                exception = Assert.Throws<ArgumentException>(() => backupConfiguration.AssertDestinationAllowed(destination));
                Assert.Equal("Backups are not allowed in this RavenDB server. Contact the administrator for more information.", exception.Message);
            }
        }

        [Fact]
        public void CanGetGenerateTheCorrectBackupName()
        {
            var configuration = new PeriodicBackupConfiguration
            {
                LocalSettings = new LocalSettings(),
                S3Settings = new S3Settings(),
                GlacierSettings = new GlacierSettings(),
                AzureSettings = new AzureSettings(),
                GoogleCloudSettings = new GoogleCloudSettings(),
                FtpSettings = new FtpSettings(),
            };

            var allDestinations = new HashSet<string>(Enum.GetValues(typeof(PeriodicBackupConfiguration.BackupDestination))
                .Cast<PeriodicBackupConfiguration.BackupDestination>().Where(x => x != PeriodicBackupConfiguration.BackupDestination.None)
                .Select(backupDestination =>
                {
                    var str = backupDestination.ToString();
                    var fieldInfo = typeof(PeriodicBackupConfiguration.BackupDestination).GetField(str);
                    var attributes = (DescriptionAttribute[])fieldInfo.GetCustomAttributes(typeof(DescriptionAttribute), false);
                    return attributes.Length > 0 ? attributes[0].Description : str;
                }));

            var destinations = configuration.GetFullBackupDestinations();

            Assert.True(allDestinations.SequenceEqual(destinations));
        }
    }
}
