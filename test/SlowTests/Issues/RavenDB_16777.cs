using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16777 : RavenTestBase
    {
        public RavenDB_16777(ITestOutputHelper output) : base(output)
        {
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanPutEmptyStringInRemoteFolderProperty()
        {
            using var server = GetNewServer();
            using var store = GetDocumentStore(new Options()
            {
                Server = server
            });

            var result = await store.Maintenance.Server.SendAsync(new PutServerWideBackupConfigurationOperation(new ServerWideBackupConfiguration
            {
                FullBackupFrequency = "0 2 * * 0",
                AzureSettings = new AzureSettings()
                {
                    AccountKey = "q",
                    AccountName = "w",
                    RemoteFolderName = string.Empty,
                    StorageContainer = "322"
                },
                FtpSettings = new FtpSettings()
                {
                    Url = string.Empty,
                },
                GlacierSettings = new GlacierSettings()
                {
                    RemoteFolderName = string.Empty,
                    AwsAccessKey = "q",
                    AwsSecretKey = "w"
                },
                GoogleCloudSettings = new GoogleCloudSettings()
                {
                    RemoteFolderName = string.Empty,
                    BucketName = "b",
                    GoogleCredentialsJson = "{}"
                },
                S3Settings = new S3Settings()
                {
                    RemoteFolderName = string.Empty,
                    AwsAccessKey = "q",
                    AwsSecretKey = "w"
                }
            }));

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Equal(1, record.PeriodicBackups.Count);

            var pb = record.PeriodicBackups.FirstOrDefault();
            Assert.NotNull(pb);

            Assert.True(pb.AzureSettings.RemoteFolderName.StartsWith("/") == false);
            Assert.True(pb.FtpSettings.Url.StartsWith("/") == false);
            Assert.True(pb.GlacierSettings.RemoteFolderName.StartsWith("/") == false);
            Assert.True(pb.GoogleCloudSettings.RemoteFolderName.StartsWith("/") == false);
            Assert.True(pb.S3Settings.RemoteFolderName.StartsWith("/") == false);
        }
    }
}
