using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
    public class FtpBasicTests : CloudBackupTestBase
    {
        public FtpBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanUploadFile()
        {
            var services = new ServiceCollection();
            services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
            services.AddFtpServer(builder => builder.UseInMemoryFileSystem().EnableAnonymousAuthentication());
            services.Configure<FtpServerOptions>(opt =>
            {
                opt.ServerAddress = "127.0.0.1";
                opt.Port = 0;
            });
            await using (var serviceProvider = services.BuildServiceProvider())
            {
                var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                try
                {
                    await ftpServerHost.StartAsync(CancellationToken.None);
                    var ftpServer = serviceProvider.GetRequiredService<IFtpServer>();
                    var settings = new FtpSettings
                    {
                        Url = $"ftp://127.0.0.1:{ftpServer.Port}/testing2",
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
                }
                finally
                {
                    await ftpServerHost.StopAsync(CancellationToken.None);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanUploadBackup()
        {
            var services = new ServiceCollection();
            services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
            services.AddFtpServer(builder => builder.UseInMemoryFileSystem().EnableAnonymousAuthentication());
            services.Configure<FtpServerOptions>(opt =>
            {
                opt.ServerAddress = "127.0.0.1";
                opt.Port = 0;
            });
            await using (var serviceProvider = services.BuildServiceProvider())
            {
                var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                try
                {
                    await ftpServerHost.StartAsync(CancellationToken.None);
                    var ftpServer = serviceProvider.GetRequiredService<IFtpServer>();
                    var settings = new FtpSettings
                    {
                        Url = $"ftp://127.0.0.1:{ftpServer.Port}/internal",
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
                        var backupId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                        var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                        var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupId, store, expectedEtag: lastEtag);
                        var backupResult = (BackupResult)store.Maintenance.Send(new GetOperationStateOperation(await Backup.GetBackupOperationIdAsync(store, backupId))).Result;
                        var isExist = CheckBackupFile(settings.Url, status.FolderName, client);
                        Assert.NotNull(backupResult);
                        Assert.Equal(UploadState.Done, backupResult.FtpBackup.UploadProgress.UploadState);
                        Assert.Equal(true, isExist);
                    }
                }
                finally
                {
                    await ftpServerHost.StopAsync(CancellationToken.None);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanUploadBackupsWithDeletion()
        {
            using (await Retention.SkipMinimumBackupAgeToKeepValidationAsync())
            {
                var services = new ServiceCollection();
                services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
                services.AddFtpServer(builder => builder.UseInMemoryFileSystem().EnableAnonymousAuthentication());
                services.Configure<FtpServerOptions>(opt =>
                {
                    opt.ServerAddress = "127.0.0.1";
                    opt.Port = 0;
                });
                await using (var serviceProvider = services.BuildServiceProvider())
                {
                    var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                    try
                    {
                        await ftpServerHost.StartAsync(CancellationToken.None);
                        var ftpServer = serviceProvider.GetRequiredService<IFtpServer>();
                        var settings = new FtpSettings
                        {
                            Url = $"ftp://127.0.0.1:{ftpServer.Port}/internal",
                            UserName = "anonymous",
                            Password = "itay@ravendb.net"
                        };
                        using (var client = new RavenFtpClient(settings))
                        using (var store = GetDocumentStore())
                        {
                            var config = Backup.CreateBackupConfiguration(ftpSettings: settings, name: "ftpBackupTest",
                                retentionPolicy: new RetentionPolicy { MinimumBackupAgeToKeep = TimeSpan.FromSeconds(15) });
                            var backupId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                            for (int i = 0; i < 3; i++)
                            {
                                using (var session = store.OpenSession())
                                {
                                    session.Store(new User { Name = "itay" });
                                    session.SaveChanges();
                                }

                                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                                await Backup.RunBackupAndReturnStatusAsync(Server, backupId, store, expectedEtag: lastEtag, timeout: 120000);
                            }
                            await Task.Delay(TimeSpan.FromSeconds(15) + TimeSpan.FromSeconds(3));
                            using (var session = store.OpenAsyncSession())
                            {
                                await session.StoreAsync(new User { Name = "itay" });
                                await session.SaveChangesAsync();
                            }
                            var etag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                            await Backup.RunBackupAndReturnStatusAsync(Server, backupId, store, isFullBackup: true, expectedEtag: etag, timeout: 120000);
                            var folders = client.GetFolders();
                            var foundFolders = 0;
                            for (int i = 0; i < folders.Count; i++)
                            {
                                var isExist = folders[i].Contains("CanUploadBackupsWithDeletion");
                                if (isExist)
                                    foundFolders++;
                            }
                            Assert.Equal(1, foundFolders);
                        }
                    }
                    finally
                    {
                        await ftpServerHost.StopAsync(CancellationToken.None);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanUploadFileOnEncrypted()
        {
            using (await Retention.SkipMinimumBackupAgeToKeepValidationAsync())
            {
                var generatedCert = Certificates.GenerateAndSaveSelfSignedCertificate();
                var setupCert = Certificates.SetupServerAuthentication(certificates: generatedCert);
                var services = new ServiceCollection();
                services.Configure<AuthTlsOptions>(cfg => cfg.ServerCertificate = generatedCert.ServerCertificate.Value);
                services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
                services.AddFtpServer(builder => builder
                    .UseInMemoryFileSystem()
                    .EnableAnonymousAuthentication());
                services.Configure<FtpServerOptions>(opt =>
                {
                    opt.ServerAddress = "127.0.0.1";
                    opt.Port = 0;
                });
                await using (var serviceProvider = services.BuildServiceProvider())
                {
                    var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                    try
                    {
                        await ftpServerHost.StartAsync(CancellationToken.None);
                        var ftpServer = serviceProvider.GetRequiredService<IFtpServer>();
                        var settings = new FtpSettings
                        {
                            Url = $"ftps://127.0.0.1:{ftpServer.Port}/internal",
                            UserName = "anonymous",
                            Password = "itay@ravendb.net",
                            CertificateAsBase64 = Convert.ToBase64String(generatedCert.ServerCertificate.Value.Export(X509ContentType.Cert))
                        };
                        using (var client = new RavenFtpClient(settings))
                        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("abc")))
                        {
                            client.UploadFile("testFolder", "testFile", stream);
                            var isExist = CheckFile(settings.Url, "testFolder", "testFile", client);
                            Assert.Equal(true, isExist);
                        }
                    }
                    finally
                    {
                        await ftpServerHost.StopAsync(CancellationToken.None);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanUploadBackupOnEncrypted()
        {
            var generatedCert = Certificates.GenerateAndSaveSelfSignedCertificate();
            var setupCert = Certificates.SetupServerAuthentication(certificates: generatedCert);
            var services = new ServiceCollection();
            services.Configure<AuthTlsOptions>(cfg => cfg.ServerCertificate = generatedCert.ServerCertificate.Value);
            services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
            services.AddFtpServer(builder => builder
                .UseInMemoryFileSystem()
                .EnableAnonymousAuthentication());
            services.Configure<FtpServerOptions>(opt =>
            {
                opt.ServerAddress = "127.0.0.1";
                opt.Port = 0;
            });
            await using (var serviceProvider = services.BuildServiceProvider())
            {
                var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                try
                {
                    await ftpServerHost.StartAsync(CancellationToken.None);
                    var ftpServer = serviceProvider.GetRequiredService<IFtpServer>();
                    var settings = new FtpSettings
                    {
                        Url = $"ftps://127.0.0.1:{ftpServer.Port}",
                        UserName = "anonymous",
                        Password = "itay@ravendb.net",
                        CertificateAsBase64 = Convert.ToBase64String(generatedCert.ServerCertificate.Value.Export(X509ContentType.Cert))
                    };
                    using (var client = new RavenFtpClient(settings))
                    using (var store = GetDocumentStore(options: new Options
                    {
                        ClientCertificate = generatedCert.ServerCertificate.Value
                    }))
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
                        var status = await Backup.RunBackupAndReturnStatusAsync(Server, backupId, store, expectedEtag: lastEtag);
                        var isExist = CheckBackupFile(settings.Url, status.FolderName, client);
                        Assert.NotNull(backupResult);
                        Assert.Equal(UploadState.Done, backupResult.FtpBackup.UploadProgress.UploadState);
                        Assert.Equal(true, isExist);
                    }
                }
                finally
                {
                    await ftpServerHost.StopAsync(CancellationToken.None);
                }
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanUploadBackupsWithDeletionOnEncrypted()
        {
            using (await Retention.SkipMinimumBackupAgeToKeepValidationAsync())
            {
                var generatedCert = Certificates.GenerateAndSaveSelfSignedCertificate();
                var setupCert = Certificates.SetupServerAuthentication(certificates: generatedCert);
                var services = new ServiceCollection();
                services.Configure<AuthTlsOptions>(cfg => cfg.ServerCertificate = generatedCert.ServerCertificate.Value);
                services.Configure<InMemoryFileSystemOptions>(opt => opt.KeepAnonymousFileSystem = true);
                services.AddFtpServer(builder => builder
                    .UseInMemoryFileSystem()
                    .EnableAnonymousAuthentication());
                services.Configure<FtpServerOptions>(opt =>
                {
                    opt.ServerAddress = "127.0.0.1";
                    opt.Port = 0;
                });
                await using (var serviceProvider = services.BuildServiceProvider())
                {
                    var ftpServerHost = serviceProvider.GetRequiredService<IFtpServerHost>();
                    try
                    {
                        await ftpServerHost.StartAsync(CancellationToken.None);
                        var ftpServer = serviceProvider.GetRequiredService<IFtpServer>();
                        var settings = new FtpSettings
                        {
                            Url = $"ftps://127.0.0.1:{ftpServer.Port}/internal",
                            UserName = "anonymous",
                            Password = "itay@ravendb.net",
                            CertificateAsBase64 = Convert.ToBase64String(generatedCert.ServerCertificate.Value.Export(X509ContentType.Cert))
                        };
                        using (var client = new RavenFtpClient(settings))
                        using (var store = GetDocumentStore(options: new Options
                        {
                            ClientCertificate = generatedCert.ServerCertificate.Value
                        }))
                        {
                            var config = Backup.CreateBackupConfiguration(ftpSettings: settings, name: "ftpBackupTest",
                                retentionPolicy: new RetentionPolicy { MinimumBackupAgeToKeep = TimeSpan.FromSeconds(15) });
                            var backupId = (await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config))).TaskId;
                            for (int i = 0; i < 3; i++)
                            {
                                using (var session = store.OpenSession())
                                {
                                    session.Store(new User { Name = "itay" });
                                    session.SaveChanges();
                                }

                                var lastEtag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                                await Backup.RunBackupAndReturnStatusAsync(Server, backupId, store, expectedEtag: lastEtag, timeout: 120000);
                            }
                            await Task.Delay(TimeSpan.FromSeconds(15) + TimeSpan.FromSeconds(3));
                            using (var session = store.OpenAsyncSession())
                            {
                                await session.StoreAsync(new User { Name = "itay" });
                                await session.SaveChangesAsync();
                            }
                            var etag = store.Maintenance.Send(new GetStatisticsOperation()).LastDocEtag;
                            await Backup.RunBackupAndReturnStatusAsync(Server, backupId, store, isFullBackup: true, expectedEtag: etag, timeout: 120000);
                            var folders = client.GetFolders();
                            var foundFolders = 0;
                            for (int i = 0; i < folders.Count; i++)
                            {
                                var isExist = folders[i].Contains("CanUploadBackupsWithDeletion");
                                if (isExist)
                                    foundFolders++;
                            }
                            Assert.Equal(1, foundFolders);
                        }
                    }
                    finally
                    {
                        await ftpServerHost.StopAsync(CancellationToken.None);
                    }
                }
            }
        }

        private bool CheckFile(string url, string folderName, string fileName, RavenFtpClient client)
        {
            var outputUrl = GetUrlAndFolders(url, out string path);

            using (var ftpClient = client.CreateFtpClient(outputUrl, keepAlive: true))
            {
                return ftpClient.FileExists(path + "/" + folderName + "/" + fileName);
            }
        }

        private bool CheckBackupFile(string url, string backupName, RavenFtpClient client)
        {
            var outputUrl = GetUrlAndFolders(url, out var path);

            using (var ftpClient = client.CreateFtpClient(outputUrl, keepAlive: false))
            {
                if (ftpClient.FileExists(path + "/" + backupName))
                    return true;
            }
            return false;
        }

        private string GetUrlAndFolders(string url, out string path)
        {
            if (url.StartsWith("ftps", StringComparison.OrdinalIgnoreCase))
            {
                url = url.Replace("ftps://", "ftp://", StringComparison.OrdinalIgnoreCase);
            }
            var uri = new Uri(url);

            var dirs = uri.AbsolutePath.TrimStart('/').TrimEnd('/').Split("/").ToList();

            url = $"{uri.Scheme}://{uri.Host}";

            path = string.Empty;
            foreach (var directory in dirs)
            {
                if (directory == string.Empty)
                    continue;

                path += $"/{directory}";
            }

            return url;
        }
    }
}
