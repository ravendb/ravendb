// -----------------------------------------------------------------------
//  <copyright file="BackupCreator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;
using FileSystemInfo = System.IO.FileSystemInfo;

namespace Raven.Tests.FileSystem.Migration
{
    public class MigrationTests : RavenFilesTestBase
    {
        [Theory(Skip = "Use this test only to create backups for migration tests")]
        [PropertyData("Storages")]
        public async Task CreateDataToMigrate(string storage)
        {
            string build = "3628"; // CHANGE THIS IF YOU CREATE A NEW BACKUP

            var source = NewAsyncClient(0, requestedStorage: storage, runInMemory: false, fileSystemName: "source");
            var destination = NewAsyncClient(1, requestedStorage: storage, runInMemory: false, fileSystemName: "destination");

            await source.Synchronization.SetDestinationsAsync(destination.ToSynchronizationDestination());

            for (int i = 0; i < 10; i++)
            {
                await source.UploadAsync(SynchronizedFileName(i), CreateUniformFileStream(i, (char) i));
            }

            SpinWait.SpinUntil(() => destination.GetStatisticsAsync().Result.FileCount == 10, TimeSpan.FromMinutes(1));

            var destinationStats = await destination.GetStatisticsAsync();

            Assert.Equal(10, destinationStats.FileCount);

            for (int i = 0; i < 10; i++)
            {
                await destination.UploadAsync(FileName(i), CreateUniformFileStream(i, (char) i));
            }

            for (int i = 0; i < 10; i++)
            {
                await destination.Configuration.SetKeyAsync(ConfigurationName(i), new RavenJObject() { { "key", string.Format("value-{0}", i) } });
            }

            await ValidateSource(source);
            await ValidateDestination(destination);

            var opId1 = await source.Admin.StartBackup(string.Format("source-{0}-{1}", build, storage), null, false, source.FileSystemName);
            await WaitForOperationAsync(source.UrlFor(), opId1);

            var opId2 = await destination.Admin.StartBackup(string.Format("destination-{0}-{1}", build, storage), null, false, destination.FileSystemName);
            await WaitForOperationAsync(destination.UrlFor(), opId2);
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task BasicMigration(string storage)
        {
            foreach (var dir in Directory.GetDirectories("../../Migration/Backups/"))
            {
                var sourcePath = NewDataPath("Source-Backup");
                var destinationPath = NewDataPath("Destination-Backup");

                foreach (var file in Directory.GetFiles(dir, string.Format("*{0}.zip", storage)).Select(x => new FileInfo(x)))
                {
                    if(file.Name.StartsWith("source"))
                        ExtractBackup(file, sourcePath);
                    else if (file.Name.StartsWith("destination"))
                        ExtractBackup(file, destinationPath);
                    else
                        throw new ArgumentException("Unknown file in backups dir" + file.Name);
                }

                using (var store = NewStore(runInMemory: false))
                {
                    var opId = await store.AsyncFilesCommands.Admin.StartRestore(new FilesystemRestoreRequest
                    {
                        BackupLocation = sourcePath,
                        FilesystemName = "source",
                        FilesystemLocation = NewDataPath("source-data")
                    });

                    await WaitForOperationAsync(store.Url, opId);


                    opId= await store.AsyncFilesCommands.Admin.StartRestore(new FilesystemRestoreRequest
                    {
                        BackupLocation = destinationPath,
                        FilesystemName = "destination",
                        FilesystemLocation = NewDataPath("destination-data")
                    });

                    await WaitForOperationAsync(store.Url, opId);

                    var source = store.AsyncFilesCommands.ForFileSystem("source");
                    var destination = store.AsyncFilesCommands.ForFileSystem("destination");

                    await source.Synchronization.SetDestinationsAsync(destination.ToSynchronizationDestination());

                    await ValidateSource(source);
                    await ValidateDestination(destination);

                    await ValidateSourceEtags(source);

                    var sourceServerId = await source.GetServerIdAsync();

                    await ValidateDestinationEtags(destination, sourceServerId);

                    for (int i = 10; i < 20; i++)
                    {
                        await source.UploadAsync(SynchronizedFileName(i), CreateRandomFileStream(i));
                    }

                    // should synchronize new files too
                    // we are using browse async instead of stats to wait for db schema update and avoid exceptions during test disposal
                    Assert.True(SpinWait.SpinUntil(() => destination.Storage.Commands.BrowseAsync().Result.Length == 30, TimeSpan.FromMinutes(1)));
                }
            }
        }

        private static async Task ValidateDestinationEtags(IAsyncFilesCommands destination, Guid sourceServeId)
        {
            var lastSynchronizedEtag = (await destination.Synchronization.GetLastSynchronizationFromAsync(sourceServeId)).LastSourceFileEtag;

            Assert.Equal(1, lastSynchronizedEtag.Restarts); // make sure that etags are valid after conversion from guids

            var sourceInfo = await destination.Configuration.GetKeyAsync<SourceSynchronizationInformation>(SynchronizationConstants.RavenSynchronizationSourcesBasePath + "/" + sourceServeId);

            Assert.Equal(1, sourceInfo.LastSourceFileEtag.Restarts); // make sure that etags are valid after conversion from guids

            foreach (var syncResultName in (await destination.Configuration.SearchAsync("SyncResult/")).ConfigNames)
            {
                var syncResult = await destination.Configuration.GetKeyAsync<SynchronizationReport>(syncResultName);

                Assert.Equal(1, syncResult.FileETag.Restarts); // make sure that etags are valid after conversion from guids
            }
        }

        private static async Task ValidateSourceEtags(IAsyncFilesCommands source)
        {
            foreach (var syncResultName in (await source.Configuration.SearchAsync("Syncing/")).ConfigNames)
            {
                var syncDetails = await source.Configuration.GetKeyAsync<SynchronizationDetails>(syncResultName);

                Assert.Equal(1, syncDetails.FileETag.Restarts); // make sure that etags are valid after conversion from guids
            }
        }

        private static void ExtractBackup(FileSystemInfo file, string extractionDirectory)
        {
            ZipFile.ExtractToDirectory(file.FullName, extractionDirectory);
        }

        private async Task ValidateSource(IAsyncFilesCommands source)
        {
            var stats = await source.GetStatisticsAsync();

            Assert.Equal(10, stats.FileCount);
            Assert.Equal(0, stats.ActiveSyncs.Count);
            Assert.Equal(0, stats.PendingSyncs.Count);

            var headers = await source.BrowseAsync();
            Assert.Equal(10, headers.Length);

            for (int i = 0; i < 10; i++)
            {
                var metadata = new Reference<RavenJObject>();
                var stream = await source.DownloadAsync(SynchronizedFileName(i), metadata);

                Assert.Equal(stream.GetMD5Hash(), CreateUniformFileStream(i, (char) i).GetMD5Hash());
            }

            var search = await source.SearchAsync("");
            Assert.Equal(10, search.Files.Count);
            Assert.Equal(10, search.FileCount);
        }

        private async Task ValidateDestination(IAsyncFilesCommands destination)
        {
            var stats = await destination.GetStatisticsAsync();

            Assert.Equal(20, stats.FileCount);

            var headers = await destination.BrowseAsync();
            Assert.Equal(20, headers.Length);

            for (int i = 0; i < 10; i++)
            {
                var metadata = new Reference<RavenJObject>();
                var stream = await destination.DownloadAsync(SynchronizedFileName(i), metadata);

                Assert.Equal(stream.GetMD5Hash(), CreateUniformFileStream(i, (char) i).GetMD5Hash());

                stream = await destination.DownloadAsync(FileName(i), metadata);

                Assert.Equal(stream.GetMD5Hash(), CreateUniformFileStream(i, (char) i).GetMD5Hash());

                var searchResult = await destination.SearchAsync("ETag:" + metadata.Value[Constants.MetadataEtagField]);

                Assert.Equal(1, searchResult.Files.Count);
                Assert.Equal(1, searchResult.FileCount);

                var config = await destination.Configuration.GetKeyAsync<RavenJObject>(ConfigurationName(i));

                Assert.Equal(string.Format("value-{0}", i), config["key"].ToString());
            }
        }

        private static string SynchronizedFileName(int i)
        {
            return string.Format("source.{0}.file", i);
        }

        private static string FileName(int i)
        {
            return string.Format("{0}.file", i);
        }

        private static string ConfigurationName(int i)
        {
            return string.Format("Configurations/{0}", i);
        }
    }
}
