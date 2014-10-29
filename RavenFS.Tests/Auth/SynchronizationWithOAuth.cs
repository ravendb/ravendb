// -----------------------------------------------------------------------
//  <copyright file="SynchronizationWithOAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Server;
using RavenFS.Tests.Synchronization;
using RavenFS.Tests.Synchronization.IO;
using Xunit;
using Raven.Abstractions.FileSystem;

namespace RavenFS.Tests.Auth
{
    public class SynchronizationWithOAuth : RavenFsTestBase
    {
        private const string apiKey = "test/ThisIsMySecret";

        protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
        {
            if (server.SystemDatabase.Configuration.Port == Ports[1]) // setup only for destination
            {
                server.SystemDatabase.Documents.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = "test",
                    Secret = "ThisIsMySecret",
                    Enabled = true,
                    Databases = new List<ResourceAccess>
                    {
                        new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = true}, // required to create file system
						new ResourceAccess {TenantId = fileSystemName}
                    }
                }), new RavenJObject(), null);
            }
        }

        [Fact]
        public async Task CanSynchronizeFileContent()
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1, enableAuthentication: true, apiKey: apiKey);

            var ms = new MemoryStream(new byte[] {3, 2, 1});

            await source.UploadAsync("ms.bin", ms);

            var result = await source.Synchronization.StartAsync("ms.bin", destination);

            Assert.Null(result.Exception);
            Assert.Equal(SynchronizationType.ContentUpdate, result.Type);
        }

        [Fact]
        public async Task CanSynchronizeMetadata()
        {
            var content = new MemoryStream(new byte[] { 1, 2, 3, 4 });

            var sourceClient = NewAsyncClient(0);
            var destinationClient = NewAsyncClient(1, enableAuthentication: true, apiKey: apiKey);

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
            var destinationClient = NewAsyncClient(1, enableAuthentication: true, apiKey: apiKey);

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
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1, enableAuthentication: true, apiKey: apiKey);

            await source.UploadAsync("test.bin", new RandomStream(1));

            var report = await source.Synchronization.StartAsync("test.bin", destination);

            Assert.Null(report.Exception);

            await source.DeleteAsync("test.bin");

            var synchronizationReport = await source.Synchronization.StartAsync("test.bin", destination);

            Assert.Equal(SynchronizationType.Delete, synchronizationReport.Type);
            Assert.Null(synchronizationReport.Exception);
        }
    }
}