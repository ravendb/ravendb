using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentFTP;
using FubarDev.FtpServer;
using FubarDev.FtpServer.FileSystem.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class Ftp : CloudBackupTestBase
    {
        public Ftp(ITestOutputHelper output) : base(output)
        {
        }

        [FtpFact]
        public async Task CanUploadFile()
        {
            var services = new ServiceCollection();
            services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
            services.AddFtpServer(builder => builder.UseInMemoryFileSystem().EnableAnonymousAuthentication());
            services.Configure<FtpServerOptions>(opt => opt.ServerAddress = "127.0.0.1");
            await using (var serviceProvider = services.BuildServiceProvider())
            {
                var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                ftpServerHost.StartAsync(CancellationToken.None).Wait();
                var settings = new FtpSettings
                {
                    Url = "ftp://127.0.0.1",
                    UserName = "anonymous",
                    Password = "itay@ravendb.net"
                };
                using (var client = new RavenFtpClient(settings))
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("abc")))
                {
                    client.UploadFile("testFolder", "testFile", stream);
                    var isExist = CheckFile(settings.Url, "testFolder", "testFile", client);
                    Assert.Equal(true, isExist);
                }
                ftpServerHost.StopAsync(CancellationToken.None).Wait();
            }
        }

        [FtpFact]
        public async Task CanUploadBackup()
        {
            var services = new ServiceCollection();
            services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
            services.AddFtpServer(builder => builder.UseInMemoryFileSystem().EnableAnonymousAuthentication());
            services.Configure<FtpServerOptions>(opt => opt.ServerAddress = "127.0.0.1");
            await using (var serviceProvider = services.BuildServiceProvider())
            {
                var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                ftpServerHost.StartAsync(CancellationToken.None).Wait();
                var settings = new FtpSettings
                {
                    Url = "ftp://127.0.0.1",
                    UserName = "anonymous",
                    Password = "itay@ravendb.net"
                };
                using (var client = new RavenFtpClient(settings))
                using (var store = GetDocumentStore())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User { Name = "itay" }, "users/1");
                        session.SaveChanges();
                    }
                    var config = Backup.CreateBackupConfiguration(ftpSettings: settings, name: "ftpBackupTest");
                    var backupId = Backup.UpdateConfigAndRunBackup(Server, config, store);
                    var backupResult = (BackupResult)store.Maintenance.Send(new GetOperationStateOperation(await Backup.GetBackupOperationIdAsync(store, backupId))).Result;
                    var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                    var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupId, store,  expectedEtag: lastEtag);
                    var isExist = CheckBackupFile(settings.Url, status.FolderName, client);
                    Assert.NotNull(backupResult);
                    Assert.Equal(UploadState.Done, backupResult.FtpBackup.UploadProgress.UploadState);
                    Assert.Equal(true, isExist);
                }

                ftpServerHost.StopAsync(CancellationToken.None).Wait();
            }
        }

        private bool CheckFile(string url, string folderName, string fileName, RavenFtpClient client)
        {
            using (var ftpClient = client.CreateFtpClient(url, keepAlive: true))
            {
                if (ftpClient.FileExists("/" + folderName + "/" + fileName))
                    return true;
            }
            return false;
        }

        private bool CheckBackupFile(string url, string backupName, RavenFtpClient client)
        {
            using (var ftpClient = client.CreateFtpClient(url, keepAlive: true))
            {
                var items = ftpClient.GetListing();
                items.OrderBy(i => i.Modified);
                foreach (var item in items)
                {
                    if (item.Type == FtpObjectType.Directory)
                    {
                        if (item.FullName.Contains(backupName))
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }
}
