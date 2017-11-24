// -----------------------------------------------------------------------
//  <copyright file="AutomaticConflictResolutions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Replication;
using Raven.Client.FileSystem;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Multipart;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Tests.FileSystem.Synchronization
{
    public class AutomaticConflictResolutions : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLocal_ContentUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLocal
            });

            var content = await ExecuteRawSynchronizationRequest(sourceClient, destinationClient);

            Assert.Equal("destination", content);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfRemote_ContentUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToRemote
            });

            var content = await ExecuteRawSynchronizationRequest(sourceClient, destinationClient);

            Assert.Equal("source", content);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLatest_ContentUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLatest
            });

            var content = await ExecuteRawSynchronizationRequest(sourceClient, destinationClient, () => Thread.Sleep(1000));

            Assert.Equal("source", content);
        }

        private static async Task<string> ExecuteRawSynchronizationRequest(IAsyncFilesCommands sourceClient, IAsyncFilesCommands destinationClient, Action action = null)
        {
            await destinationClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("destination")));

            if (action != null)
                action();

            await sourceClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("source")));

            var sourceStream = new MemoryStream();

            (await sourceClient.DownloadAsync("test")).CopyTo(sourceStream);

            var metadata = await sourceClient.GetMetadataForAsync("test");

            var request = new SynchronizationMultipartRequest(destinationClient.Synchronization, new FileSystemInfo()
            {
                Url = sourceClient.UrlFor(),
                Id = sourceClient.GetServerIdAsync().Result
            }, "test", metadata, sourceStream, new[]
            {
                new RdcNeed()
                {
                    BlockLength = 6,
                    BlockType = RdcNeedType.Source,
                    FileOffset = 0
                }
            }, SynchronizationType.ContentUpdate);

            var synchronizationReport = await request.PushChangesAsync(CancellationToken.None);

            Assert.Null(synchronizationReport.Exception);

            var stream = await destinationClient.DownloadAsync("test");

            return StreamToString(stream);
        }

        private static async Task<string> RunContentSynchronization(IAsyncFilesCommands sourceClient, IAsyncFilesCommands destinationClient, Action action = null)
        {
            await destinationClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("destination")));

            if (action != null)
                action();

            await sourceClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("source")));

            var report = await sourceClient.Synchronization.StartAsync("test", destinationClient);

            Assert.Null(report.Exception);
            Assert.Equal(SynchronizationType.ContentUpdate, report.Type);

            var stream = await destinationClient.DownloadAsync("test");

            return StreamToString(stream);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLocal_DuringSynchronization_ContentUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLocal
            });

            var content = await RunContentSynchronization(sourceClient, destinationClient);

            Assert.Equal("destination", content);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfRemote_DuringSynchronization_ContentUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToRemote
            });

            var content = await RunContentSynchronization(sourceClient, destinationClient);

            Assert.Equal("source", content);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLatest_DuringSynchronization_ContentUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLatest
            });

            var content = await RunContentSynchronization(sourceClient, destinationClient, () => Thread.Sleep(1000));

            Assert.Equal("source", content);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLocal_DuringSynchronization_Rename()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLocal
            });

            await RunRenameSynchronization(sourceClient, destinationClient);

            Assert.NotNull(await destinationClient.GetMetadataForAsync("test"));
            Assert.Null(await destinationClient.GetMetadataForAsync("renamed"));
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfRemote_DuringSynchronization_Rename()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToRemote
            });

            await RunRenameSynchronization(sourceClient, destinationClient);

            Assert.Null(await destinationClient.GetMetadataForAsync("test"));
            Assert.NotNull(await destinationClient.GetMetadataForAsync("renamed"));
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLatest_DuringSynchronization_Rename()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLatest
            });

            await RunRenameSynchronization(sourceClient, destinationClient, () => Thread.Sleep(1000));

            Assert.Null(await destinationClient.GetMetadataForAsync("test"));
            Assert.NotNull(await destinationClient.GetMetadataForAsync("renamed"));
        }

        private static async Task RunRenameSynchronization(IAsyncFilesCommands sourceClient, IAsyncFilesCommands destinationClient, Action action = null)
        {
            await destinationClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("rename-test")));

            if (action != null)
                action();

            await sourceClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("rename-test")));

            await sourceClient.RenameAsync("test", "renamed");

            var report = await sourceClient.Synchronization.StartAsync("test", destinationClient);

            Assert.Null(report.Exception);
            Assert.Equal(SynchronizationType.Rename, report.Type);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLocal_DuringSynchronization_MetadataUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLocal
            });

            await RunMetadataSynchronization(sourceClient, destinationClient);

            Assert.Equal("destination", (await destinationClient.GetMetadataForAsync("test")).Value<string>("Sample-Header"));
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfRemote_DuringSynchronization_MetadataUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToRemote
            });

            await RunMetadataSynchronization(sourceClient, destinationClient);

            Assert.Equal("source", (await destinationClient.GetMetadataForAsync("test")).Value<string>("Sample-Header"));
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLatest_DuringSynchronization_MetadataUpdate()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLatest
            });

            await RunMetadataSynchronization(sourceClient, destinationClient, () => Thread.Sleep(1000));

            Assert.Equal("source", (await destinationClient.GetMetadataForAsync("test")).Value<string>("Sample-Header"));
        }

        private static async Task RunMetadataSynchronization(IAsyncFilesCommands sourceClient, IAsyncFilesCommands destinationClient, Action action = null)
        {
            await destinationClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("metadata-test")), new RavenJObject { { "Sample-Header", "destination" } });

            if (action != null)
                action();

            await sourceClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("metadata-test")), new RavenJObject { { "Sample-Header", "source" } });

            var report = await sourceClient.Synchronization.StartAsync("test", destinationClient);

            Assert.Null(report.Exception);
            Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLocal_DuringSynchronization_Delete()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLocal
            });

            await RunDeleteSynchronization(sourceClient, destinationClient);

            Assert.NotNull(await destinationClient.GetMetadataForAsync("test"));
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfRemote_DuringSynchronization_Delete()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToRemote
            });

            await RunDeleteSynchronization(sourceClient, destinationClient);

            Assert.Null(await destinationClient.GetMetadataForAsync("test"));
        }

        [Fact]
        public async Task ShouldAutomaticallyResolveInFavourOfLatest_DuringSynchronization_Delete()
        {
            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1);

            await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
            {
                FileConflictResolution = StraightforwardConflictResolution.ResolveToLatest
            });

            await RunDeleteSynchronization(sourceClient, destinationClient);

            Assert.Null(await destinationClient.GetMetadataForAsync("test"));
        }

        private static async Task RunDeleteSynchronization(IAsyncFilesCommands sourceClient, IAsyncFilesCommands destinationClient, Action action = null)
        {
            await destinationClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("delete-test")));

            if (action != null)
                action();

            await sourceClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("delete-test")));
            await sourceClient.DeleteAsync("test");

            var report = await sourceClient.Synchronization.StartAsync("test", destinationClient);

            Assert.Null(report.Exception);
            Assert.Equal(SynchronizationType.Delete, report.Type);
        }
    }
}
