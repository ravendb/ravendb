using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;
using ClientBackupConfiguration = Raven.Client.Documents.Operations.Backups.BackupConfiguration;

namespace FastTests.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupTests : RavenTestBase
    {
        public PeriodicBackupTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport, LicenseRequired = true)]
        public async Task CanSetupPeriodicBackupWithVeryLargePeriods()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            using (var store = GetDocumentStore())
            {
                var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "* */1 * * *", incrementalBackupFrequency: "* */2 * * *");
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var periodicBackupRunner = (await Databases.GetDocumentDatabaseInstanceFor(store)).PeriodicBackupRunner;
                var backups = periodicBackupRunner.PeriodicBackups;
                Assert.Equal("* */1 * * *", backups.First().Configuration.FullBackupFrequency);
                Assert.Equal("* */2 * * *", backups.First().Configuration.IncrementalBackupFrequency);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

        [RavenFact(RavenTestCategory.BackupExportImport)]
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

        // a one time backup used to accept an invalid direct upload configuration and to fail only once the backup was running
        [RavenTheory(RavenTestCategory.BackupExportImport, LicenseRequired = true)]
        [InlineData(true, "two-destinations")]
        [InlineData(true, "local-folder")]
        [InlineData(true, "unsupported-destination")]
        [InlineData(false, "two-destinations")]
        [InlineData(false, "local-folder")]
        [InlineData(false, "unsupported-destination")]
        public void Invalid_direct_upload_configuration_is_rejected_when_the_backup_is_requested(bool oneTimeBackup, string scenario)
        {
            using (var store = GetDocumentStore())
            {
                Exception e;
                string expectedError;

                if (oneTimeBackup)
                {
                    var configuration = ConfigureInvalidDirectUpload(new ClientBackupConfiguration(), scenario, out expectedError);
                    e = Assert.ThrowsAny<Exception>(() => store.Maintenance.Send(new BackupOperation(configuration)));
                }
                else
                {
                    var configuration = ConfigureInvalidDirectUpload(new PeriodicBackupConfiguration { FullBackupFrequency = "0 0 1 1 *" }, scenario, out expectedError);
                    e = Assert.ThrowsAny<Exception>(() => store.Maintenance.Send(new UpdatePeriodicBackupOperation(configuration)));
                }

                Assert.Contains(expectedError, e.ToString());
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport, LicenseRequired = true)]
        public void Valid_one_time_direct_upload_configuration_is_accepted_when_the_backup_is_requested()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new ClientBackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    BackupUploadMode = BackupUploadMode.DirectUpload,
                    // invalid credentials, so the backup fails as soon as the upload starts, without going to the network
                    GoogleCloudSettings = new GoogleCloudSettings { BucketName = "bucket", GoogleCredentialsJson = "not a credentials json" }
                };

                var operation = store.Maintenance.Send(new BackupOperation(configuration));

                var e = Assert.ThrowsAny<Exception>(() => operation.WaitForCompletion(TimeSpan.FromMinutes(1)));
                Assert.Contains("Wrong format for Google Credentials", e.ToString());
            }
        }

        private T ConfigureInvalidDirectUpload<T>(T configuration, string scenario, out string expectedError) where T : ClientBackupConfiguration
        {
            configuration.BackupType = BackupType.Backup;
            configuration.BackupUploadMode = BackupUploadMode.DirectUpload;

            switch (scenario)
            {
                case "two-destinations":
                    configuration.S3Settings = new S3Settings { BucketName = "bucket", AwsRegionName = "us-east-1", AwsAccessKey = "key", AwsSecretKey = "secret" };
                    configuration.GoogleCloudSettings = new GoogleCloudSettings { BucketName = "bucket", GoogleCredentialsJson = "{}" };
                    expectedError = "Cannot use direct upload when we configure more than one destination";
                    break;

                case "local-folder":
                    configuration.LocalSettings = new LocalSettings { FolderPath = NewDataPath(suffix: "BackupFolder") };
                    configuration.S3Settings = new S3Settings { BucketName = "bucket", AwsRegionName = "us-east-1", AwsAccessKey = "key", AwsSecretKey = "secret" };
                    expectedError = "Trying to use direct upload when we configure a backup to a local folder";
                    break;

                case "unsupported-destination":
                    configuration.FtpSettings = new FtpSettings { Url = "ftp://localhost:21" };
                    expectedError = "No supported backup destination for direct upload was set";
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(scenario), scenario, null);
            }

            return configuration;
        }
    }
}