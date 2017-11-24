// -----------------------------------------------------------------------
//  <copyright file="SynchronizationWithOAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.FileSystem.Synchronization;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Auth
{
    public class SynchronizationWithOAuth : RavenFilesTestWithLogs
    {
        private const string Name = "test";
        private const string Secret = "ThisIsMySecret";
        private const string ApiKey = Name + "/" + Secret;

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            if (server.SystemDatabase.Configuration.Port == Ports[1]) // setup only for destination
            {
                server.SystemDatabase.Documents.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = Name,
                    Secret = Secret,
                    Enabled = true,
                    Databases = new List<ResourceAccess>
                    {
                        new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true}, // required to create file system
                        new ResourceAccess {TenantId = fileSystemName}
                    }
                }), new RavenJObject(), null);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(false)]
        public async Task CanSynchronizeFileContent(bool disableRdc)
        {
            var sourceClient = (IAsyncFilesCommandsImpl)NewAsyncClient(0);
            GetFileSystem(0).Configuration.FileSystem.DisableRDC = disableRdc;

            var destination = NewAsyncClient(1, enableAuthentication: true, apiKey: ApiKey);

            var ms = new MemoryStream(new byte[] {3, 2, 1});

            await sourceClient.UploadAsync("ms.bin", ms);

            var result = await sourceClient.Synchronization.StartAsync("ms.bin", destination);

            Assert.Null(result.Exception);
            Assert.Equal(disableRdc ? SynchronizationType.ContentUpdateNoRDC : SynchronizationType.ContentUpdate, result.Type);
        }

        [Fact]
        public async Task CanSynchronizeMetadata()
        {
            var content = new MemoryStream(new byte[] { 1, 2, 3, 4 });

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1, enableAuthentication: true, apiKey: ApiKey);

            await sourceClient.UploadAsync("test.bin", content, new RavenJObject { { "difference", "metadata" } });
            content.Position = 0;
            await destinationClient.UploadAsync("test.bin", content, new RavenJObject { { "really", "different" } });

            var report = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

            Assert.Null(report.Exception);
            Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);

            var destinationMetadata = destinationClient.GetMetadataForAsync("test.bin").Result;

            Assert.Equal("metadata", destinationMetadata["difference"]);
        }

        [Fact]
        public async Task CanSynchronizeFileRename()
        {
            var content = new MemoryStream(new byte[] { 1, 2, 3, 4 });

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1, enableAuthentication: true, apiKey: ApiKey);

            await sourceClient.UploadAsync("test.bin",  content);
            content.Position = 0;
            await destinationClient.UploadAsync("test.bin", content);

            await sourceClient.RenameAsync("test.bin", "renamed.bin");

            // we need to indicate old file name, otherwise content update would be performed because renamed file does not exist on dest
            var report = SyncTestUtils.ResolveConflictAndSynchronize(sourceClient, destinationClient, "test.bin");

            Assert.Null(report.Exception);
            Assert.Equal(SynchronizationType.Rename, report.Type);

            var testMetadata = await destinationClient.GetMetadataForAsync("test.bin");
            var renamedMetadata = await destinationClient.GetMetadataForAsync("renamed.bin");

            Assert.Null(testMetadata);
            Assert.NotNull(renamedMetadata);
        }

        [Fact]
        public async Task CanSynchronizeFileDelete()
        {
            var sourceClient = (IAsyncFilesCommandsImpl)NewAsyncClient(0);
            var destination = NewAsyncClient(2);

            await sourceClient.UploadAsync("test.bin", new RandomStream(1));

            var report = await sourceClient.Synchronization.StartAsync("test.bin", destination);

            Assert.Null(report.Exception);

            await sourceClient.DeleteAsync("test.bin");

            var synchronizationReport = await sourceClient.Synchronization.StartAsync("test.bin", destination);

            Assert.Equal(SynchronizationType.Delete, synchronizationReport.Type);
            Assert.Null(synchronizationReport.Exception);
        }
    }
}
