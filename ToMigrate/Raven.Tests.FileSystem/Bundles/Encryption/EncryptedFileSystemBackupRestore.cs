// -----------------------------------------------------------------------
//  <copyright file="EncryptedFileSystemBackupRestore.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Client.Extensions;
using Raven.Client.FileSystem;
using Raven.Database.Extensions;
using Raven.Tests.Common.Util;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Bundles.Encryption
{
    public class EncryptedFileSystemBackupRestore : FileSystemEncryptionTest
    {
        private readonly string backupDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "BackupRestoreTests.Backup");
        private new string dataPath;

        public EncryptedFileSystemBackupRestore()
        {
            IOExtensions.DeleteDirectory(backupDir);
        }

        public override void Dispose()
        {
            base.Dispose();

            IOExtensions.DeleteDirectory(backupDir);

            if(dataPath != null)
                IOExtensions.DeleteDirectory(dataPath);
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task CanRestoreBackupIfEncryptionEnabledOnServer(string requestedStorage)
        {
            using (var client = NewAsyncClientForEncryptedFs(requestedStorage))
            {
                var server = GetServer();

                string filesystemDir = Path.Combine(server.Configuration.FileSystem.DataDirectory, "NewFS");

                await CreateSampleData(client);
                // fetch md5 sums for later verification
                var md5Sums = FetchMd5Sums(client);

                // create backup
                var opId = await client.Admin.StartBackup(backupDir, null, false, client.FileSystemName);
                await WaitForOperationAsync(server.SystemDatabase.ServerUrl, opId);

                // restore newly created backup
                await client.Admin.StartRestore(new FilesystemRestoreRequest
                {
                    BackupLocation = backupDir,
                    FilesystemName = "NewFS",
                    FilesystemLocation = filesystemDir
                });

                SpinWait.SpinUntil(() => client.Admin.GetNamesAsync().Result.Contains("NewFS"),
                            Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(1));

                var restoredMd5Sums = FetchMd5Sums(client.ForFileSystem("NewFS"));
                Assert.Equal(md5Sums, restoredMd5Sums);

                var restoredClientComputedMd5Sums = ComputeMd5Sums(client.ForFileSystem("NewFS"));
                Assert.Equal(md5Sums, restoredClientComputedMd5Sums);
            }

            AssertPlainTextIsNotSavedInFileSystem("Secret", "records");
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task CanRestoreBackupOfEncryptedFileSystem(string requestedStorage)
        {
            dataPath = NewDataPath("CanRestoreBackupOfEncryptedFileSystem", false);

            using (var server = CreateServer(8079, requestedStorage: requestedStorage, runInMemory: false, dataDirectory: dataPath))
            {
                var store = server.FilesStore;

                var fs1Doc = new FileSystemDocument
                {
                    Id = Constants.FileSystem.Prefix + "FS1",
                    Settings =
                    {
                        {Constants.FileSystem.DataDirectory, Path.Combine(server.Configuration.FileSystem.DataDirectory, "FS1")},
                        {Constants.ActiveBundles, "Encryption"}
                    },
                    SecuredSettings = new Dictionary<string, string>
                    {
                        {
                            "Raven/Encryption/Key", "arHd5ENxwieUCAGkf4Rns8oPWx3f6npDgAowtIAPox0="
                        },
                        {
                            "Raven/Encryption/Algorithm", "System.Security.Cryptography.DESCryptoServiceProvider, mscorlib"
                        },
                    },
                };
                await store.AsyncFilesCommands.Admin.CreateFileSystemAsync(fs1Doc);

                using (var session = store.OpenAsyncSession("FS1"))
                {
                    session.RegisterUpload("test1.txt", StringToStream("Secret password"));
                    session.RegisterUpload("test2.txt", StringToStream("Security guard"));
                    await session.SaveChangesAsync();
                }

                var opId = await store.AsyncFilesCommands.ForFileSystem("FS1").Admin.StartBackup(backupDir, null, false, "FS1");
                await WaitForOperationAsync(server.SystemDatabase.ServerUrl, opId);

                string filesystemDir = Path.Combine(server.Configuration.FileSystem.DataDirectory, "FS2");

                await store.AsyncFilesCommands.Admin.StartRestore(new FilesystemRestoreRequest
                {
                    BackupLocation = backupDir,
                    FilesystemName = "FS2",
                    FilesystemLocation = filesystemDir
                });

                SpinWait.SpinUntil(() => store.AsyncFilesCommands.Admin.GetNamesAsync().Result.Contains("FS2"),
                            Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromMinutes(1));

                using (var session = server.DocumentStore.OpenAsyncSession(Constants.SystemDatabase))
                {
                    var fs2Doc = await session.LoadAsync<FileSystemDocument>(Constants.FileSystem.Prefix + "FS2");

                    Assert.NotEqual(fs1Doc.SecuredSettings["Raven/Encryption/Key"], fs2Doc.SecuredSettings["Raven/Encryption/Key"]);
                    Assert.NotEqual(fs1Doc.SecuredSettings["Raven/Encryption/Algorithm"], fs2Doc.SecuredSettings["Raven/Encryption/Algorithm"]);
                }

                using (var session = store.OpenAsyncSession("FS2"))
                {
                    var test1 = StreamToString(await session.DownloadAsync("test1.txt"));

                    Assert.Equal("Secret password", test1);

                    var test2 = StreamToString(await session.DownloadAsync("test2.txt"));

                    Assert.Equal("Security guard", test2);
                }
            }

            Close();

            EncryptionTestUtil.AssertPlainTextIsNotSavedInAnyFileInPath(new[]
            {
                "Secret password", "Security guard"
            }, dataPath, s => true);
        }

        private async Task CreateSampleData(IAsyncFilesCommands commands, int startIndex = 1, int count = 2)
        {
            for (var i = startIndex; i < startIndex + count; i++)
            {
                await commands.UploadAsync(string.Format("file{0}.bin", i), StringToStream("Secret records / " + i));
            }
        }

        private string[] FetchMd5Sums(IAsyncFilesCommands filesCommands, int filesCount = 2)
        {
            return Enumerable.Range(1, filesCount).Select(i =>
            {
                var meta = filesCommands.GetMetadataForAsync(string.Format("file{0}.bin", i)).Result;
                return meta.Value<string>("Content-MD5");
            }).ToArray();
        }

        private string[] ComputeMd5Sums(IAsyncFilesCommands filesCommands, int filesCount = 2)
        {
            return Enumerable.Range(1, filesCount).Select(i =>
            {
                using (var stream = filesCommands.DownloadAsync(string.Format("file{0}.bin", i)).Result)
                {
                    return stream.GetMD5Hash();
                }
            }).ToArray();
        }
    }
}
