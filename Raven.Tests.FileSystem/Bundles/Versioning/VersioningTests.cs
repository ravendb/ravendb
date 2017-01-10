// -----------------------------------------------------------------------
//  <copyright file="VersioningTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Client.FileSystem.Bundles.Versioning;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Bundles.Versioning
{
    public class VersioningTests : RavenFilesTestWithLogs
    {
        private const string Content1 = "aaa";
        private const string Content2 = "bbb";
        private const string Content3 = "ccc";
        private const string Content4 = "ddd";

        [Theory]
        [PropertyData("Storages")]
        public async Task FirstUploadWillCreateRevision(string requestedStorage)
        {
            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 10 });

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test.txt", StringToStream("abc"));

                    await session.SaveChangesAsync();

                    var revisions = await session.GetRevisionNamesForAsync("test.txt", 0, 128);

                    Assert.Equal(1, revisions.Length);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task Simple(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 10 });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content3, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
                Assert.NotNull(stream);
                Assert.Equal(Content1, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2");
                Assert.NotNull(stream);
                Assert.Equal(Content2, StreamToString(stream));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task MaxRevisions(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 3 });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content4));

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content4, StreamToString(stream));

                await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1"));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2");
                Assert.NotNull(stream);
                Assert.Equal(Content2, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/3");
                Assert.NotNull(stream);
                Assert.Equal(Content3, StreamToString(stream));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task WhenPurgeOnDeleteIsSetToFalseRevisionFilesShouldNotBeDeleted(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, PurgeOnDelete = false });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content3, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
                Assert.NotNull(stream);
                Assert.Equal(Content1, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2");
                Assert.NotNull(stream);
                Assert.Equal(Content2, StreamToString(stream));

                await store.AsyncFilesCommands.DeleteAsync(FileName);

                await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
                Assert.NotNull(stream);
                Assert.Equal(Content1, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2");
                Assert.NotNull(stream);
                Assert.Equal(Content2, StreamToString(stream));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task WhenPurgeOnDeleteIsSetToTrueRevisionFilesShouldBeDeleted(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, PurgeOnDelete = true });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content3, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
                Assert.NotNull(stream);
                Assert.Equal(Content1, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2");
                Assert.NotNull(stream);
                Assert.Equal(Content2, StreamToString(stream));

                await store.AsyncFilesCommands.DeleteAsync(FileName);

                await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName));
                await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1"));
                await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2"));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task RevisionsCannotBeDeletedWithoutProperSetting(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content2, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
                Assert.NotNull(stream);
                Assert.Equal(Content1, StreamToString(stream));

                var e = await AssertAsync.Throws<ErrorResponseException>(async () => await store.AsyncFilesCommands.DeleteAsync(FileName + "/revisions/1"));
                Assert.True(e.Message.Contains("Deleting a historical revision is not allowed"));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
                Assert.NotNull(stream);
                Assert.Equal(Content1, StreamToString(stream));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task RevisionsCanBeDeletedWithProperSetting(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning", customConfig: configuration => configuration.Settings[Constants.FileSystem.Versioning.ChangesToRevisionsAllowed] = "true"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content2, StreamToString(stream));

                stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
                Assert.NotNull(stream);
                Assert.Equal(Content1, StreamToString(stream));

                await store.AsyncFilesCommands.DeleteAsync(FileName + "/revisions/1");
                await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1"));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task GetRevisionsForAsyncShouldWork(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionsForAsync(FileName, 0, 100);
                    Assert.Equal(3, revisions.Length);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task GetRevisionsWithoutKnowingTheFileName(string requestedStorage)
        {
            const string FileName1 = "/file1.txt";
            const string FileName2 = "/file2.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync(FileName1, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName1, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName1, StringToStream(Content3));

                await store.AsyncFilesCommands.UploadAsync(FileName2, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName2, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName2, StringToStream(Content3));

                var revisions = await store.AsyncFilesCommands.StartsWithAsync("/", "*/revisions/*", 0, 128);
                Assert.Equal(6, revisions.Length);

                revisions = await store.AsyncFilesCommands.StartsWithAsync("/", "*/revisions/*", 0, 2);
                Assert.Equal(2, revisions.Length);
                Assert.True(revisions.Any(x => x.FullPath == FileName1 + "/revisions/1"));
                Assert.True(revisions.Any(x => x.FullPath == FileName1 + "/revisions/2"));

                revisions = await store.AsyncFilesCommands.StartsWithAsync("/", "*/revisions/*", 2, 5);
                Assert.Equal(4, revisions.Length);
                Assert.True(revisions.Any(x => x.FullPath == FileName1 + "/revisions/3"));
                Assert.True(revisions.Any(x => x.FullPath == FileName2 + "/revisions/1"));
                Assert.True(revisions.Any(x => x.FullPath == FileName2 + "/revisions/2"));
                Assert.True(revisions.Any(x => x.FullPath == FileName2 + "/revisions/3"));

                revisions = await store.AsyncFilesCommands.StartsWithAsync("/", "*/revisions/*", 3, 2);
                Assert.Equal(2, revisions.Length);
                Assert.True(revisions.Any(x => x.FullPath == FileName2 + "/revisions/1"));
                Assert.True(revisions.Any(x => x.FullPath == FileName2 + "/revisions/2"));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task GetRevisionNamesForAsyncShouldWork(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(FileName, 0, 100);
                    Assert.Equal(3, revisions.Length);
                    Assert.Contains(FileHeader.Canonize(FileName) + "/revisions/1", revisions);
                    Assert.Contains(FileHeader.Canonize(FileName) + "/revisions/2", revisions);
                    Assert.Contains(FileHeader.Canonize(FileName) + "/revisions/3", revisions);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task Exclude(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, Exclude = true });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(FileName, 0, 100);
                    Assert.Equal(0, revisions.Length);
                }

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content3, StreamToString(stream));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task ExcludeExplicit1(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, ExcludeUnlessExplicit = true });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(FileName, 0, 100);
                    Assert.Equal(0, revisions.Length);
                }

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content3, StreamToString(stream));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task ExcludeExplicit2(string requestedStorage)
        {
            const string FileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, ExcludeUnlessExplicit = true });

                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content1), new RavenJObject { { Constants.RavenCreateVersion, true } });
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content2), new RavenJObject { { Constants.RavenCreateVersion, true } });
                await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(Content3), new RavenJObject { { Constants.RavenCreateVersion, true } });

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(FileName, 0, 100);
                    Assert.Equal(3, revisions.Length);
                }

                var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
                Assert.NotNull(stream);
                Assert.Equal(Content3, StreamToString(stream));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task CannotModifyHistoricalRevisionByDefault_PUT(string requestedStorage)
        {
            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync("file.txt", StringToStream(Content1));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync("file.txt", 0, 100);
                    Assert.Equal(1, revisions.Length);

                    session.RegisterUpload(revisions[0], StringToStream(Content2));

                    var ex = Assert.Throws<ErrorResponseException>(() => AsyncHelpers.RunSync(() => session.SaveChangesAsync()));

                    Assert.Contains("PUT vetoed on file /file.txt/revisions/1 by Raven.Database.FileSystem.Bundles.Versioning.Plugins.VersioningPutTrigger because: Modifying a historical revision is not allowed", ex.Message);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task CanModifyHistoricalRevisionIfProperConfigurationSet_PUT(string requestedStorage)
        {
            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning", customConfig: configuration => configuration.Settings[Constants.FileSystem.Versioning.ChangesToRevisionsAllowed] = "true"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync("file.txt", StringToStream(Content1));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync("file.txt", 0, 100);
                    Assert.Equal(1, revisions.Length);

                    session.RegisterUpload(revisions[0], StringToStream(Content2));

                    await session.SaveChangesAsync();
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task CannotModifyHistoricalRevisionByDefault_POST(string requestedStorage)
        {
            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync("file.txt", StringToStream(Content1));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync("file.txt", 0, 100);
                    Assert.Equal(1, revisions.Length);

                    var revision = await session.LoadFileAsync(revisions[0]);

                    revision.Metadata.Add("new", "item");

                    var ex = Assert.Throws<ErrorResponseException>(() => AsyncHelpers.RunSync(() => session.SaveChangesAsync()));

                    Assert.Contains("POST vetoed on file /file.txt/revisions/1 by Raven.Database.FileSystem.Bundles.Versioning.Plugins.VersioningMetadataUpdateTrigger because: Modifying a historical revision is not allowed", ex.Message);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task CanModifyHistoricalRevisionIfProperConfigurationSet_POST(string requestedStorage)
        {
            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning", customConfig: configuration => configuration.Settings[Constants.FileSystem.Versioning.ChangesToRevisionsAllowed] = "true"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync("file.txt", StringToStream(Content1));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync("file.txt", 0, 100);
                    Assert.Equal(1, revisions.Length);

                    var revision = await session.LoadFileAsync(revisions[0]);

                    revision.Metadata.Add("new", "item");

                    await session.SaveChangesAsync();
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task ShouldCreateRevisionAfterMetadataUpdate(string requestedStorage)
        {
            const string fileName = "file1.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content2));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(fileName, 0, 100);
                    Assert.Equal(2, revisions.Length);
                }

                await store.AsyncFilesCommands.UpdateMetadataAsync(fileName, new RavenJObject{ { "New", "Data" } });

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(fileName, 0, 100);
                    Assert.Equal(3, revisions.Length);

                    var revisionFile = await session.LoadFileAsync(revisions[2]);

                    Assert.Equal("Data", revisionFile.Metadata.Value<string>("New"));

                    var stream = await store.AsyncFilesCommands.DownloadAsync(revisionFile.FullPath);
                    Assert.NotNull(stream);
                    Assert.Equal(Content2, StreamToString(stream));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task ShouldDeleteRevisionsAfterRenameByDefault(string requestedStorage)
        {
            const string fileName = "file1.txt";
            const string newFileName = "file2.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content3));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(fileName, 0, 100);
                    Assert.Equal(3, revisions.Length);
                }

                await store.AsyncFilesCommands.RenameAsync(fileName, newFileName);

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(newFileName, 0, 100);
                    Assert.Equal(1, revisions.Length);

                    var revisionFile = await session.LoadFileAsync(revisions[0]);

                    var stream = await store.AsyncFilesCommands.DownloadAsync(revisionFile.FullPath);
                    Assert.NotNull(stream);
                    Assert.Equal(Content3, StreamToString(stream));
                }

                // make sure that versioning keep working
                await store.AsyncFilesCommands.UploadAsync(newFileName, StringToStream(Content4));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(newFileName, 0, 100);
                    Assert.Equal(2, revisions.Length);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task ShouldRenameAllRevisions(string requestedStorage)
        {
            const string fileName = "file1.txt";
            const string newFileName = "file2.txt";

            using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, ResetOnRename = false });

                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content1));
                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content2));
                await store.AsyncFilesCommands.UploadAsync(fileName, StringToStream(Content3));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(fileName, 0, 100);
                    Assert.Equal(3, revisions.Length);
                }

                await store.AsyncFilesCommands.RenameAsync(fileName, newFileName);

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(newFileName, 0, 100);
                    Assert.Equal(3, revisions.Length);

                    var revisionFile = await session.LoadFileAsync(revisions[2]);

                    var stream = await store.AsyncFilesCommands.DownloadAsync(revisionFile.FullPath);
                    Assert.NotNull(stream);
                    Assert.Equal(Content3, StreamToString(stream));
                }

                // make sure that versioning keep working
                await store.AsyncFilesCommands.UploadAsync(newFileName, StringToStream(Content4));

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionNamesForAsync(newFileName, 0, 100);
                    Assert.Equal(4, revisions.Length);

                    var stream = await store.AsyncFilesCommands.DownloadAsync(revisions[3]);
                    Assert.NotNull(stream);
                    Assert.Equal(Content4, StreamToString(stream));
                }
            }
        }
    }
}
