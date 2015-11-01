// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1516.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.Bundles.Replication.Data;
using Raven.Database.FileSystem.Synchronization;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Common.Dto;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_3672 : RavenFilesTestWithLogs
    {
        public class WithoutAuth : RavenDB_3672
        {
            [Theory]
            [PropertyData("Storages")]
            public async Task SynchronizationTopologyDiscovererSimpleTest(string storage)
            {
                using (var store1 = NewStore(0, requestedStorage: storage))
                using (var store2 = NewStore(1, requestedStorage: storage))
                using (var store3 = NewStore(2, requestedStorage: storage))
                using (var store4 = NewStore(3, requestedStorage: storage))
                using (var store5 = NewStore(4, requestedStorage: storage))
                {
                    await store1.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store2.AsyncFilesCommands.ToSynchronizationDestination());
                    await store2.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store3.AsyncFilesCommands.ToSynchronizationDestination());
                    await store3.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store4.AsyncFilesCommands.ToSynchronizationDestination());
                    await store4.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store5.AsyncFilesCommands.ToSynchronizationDestination());
                    await store5.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store1.AsyncFilesCommands.ToSynchronizationDestination());

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.RegisterUpload("file1.bin", new MemoryStream());
                        await session.SaveChangesAsync();
                    }

                    await store1.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store2.AsyncFilesCommands, "file1.bin");

                    await store2.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store3.AsyncFilesCommands, "file1.bin");

                    await store3.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store4.AsyncFilesCommands, "file1.bin");

                    await store4.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store5.AsyncFilesCommands, "file1.bin");

                    await store5.AsyncFilesCommands.Synchronization.StartAsync();

                    var metadata = await store5.AsyncFilesCommands.GetMetadataForAsync("file1.bin");

                    Assert.NotNull(metadata);

                    var url = store1.Url.ForFilesystem(store1.DefaultFileSystem) + "/admin/synchronization/topology/view";

                    var request = store1
                        .JsonRequestFactory
                        .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Post, store1.AsyncFilesCommands.PrimaryCredentials, store1.Conventions));

                    var json = (RavenJObject)request.ReadResponseJson();
                    var topology = json.Deserialize<SynchronizationTopology>(new DocumentConvention());

                    Assert.NotNull(topology);
                    Assert.Equal(5, topology.Servers.Count);
                    Assert.Equal(5, topology.Connections.Count);

                    topology.Connections.Single(x => x.Destination == store1.Url.ForFilesystem(store1.DefaultFileSystem) && x.Source == store5.Url.ForFilesystem(store5.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store2.Url.ForFilesystem(store2.DefaultFileSystem) && x.Source == store1.Url.ForFilesystem(store1.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store3.Url.ForFilesystem(store3.DefaultFileSystem) && x.Source == store2.Url.ForFilesystem(store2.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store4.Url.ForFilesystem(store4.DefaultFileSystem) && x.Source == store3.Url.ForFilesystem(store3.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store5.Url.ForFilesystem(store5.DefaultFileSystem) && x.Source == store4.Url.ForFilesystem(store4.DefaultFileSystem));

                    foreach (var connection in topology.Connections.Where(x => x.Destination != store1.Url.ForFilesystem(store1.DefaultFileSystem) && x.Source != store5.Url.ForFilesystem(store5.DefaultFileSystem)))
                    {
                        Assert.Equal(ReplicatonNodeState.Online, connection.SourceToDestinationState);
                        Assert.Equal(ReplicatonNodeState.Online, connection.DestinationToSourceState);
                        Assert.NotNull(connection.Source);
                        Assert.NotNull(connection.Destination);
                        Assert.NotNull(connection.LastSourceFileEtag);
                        Assert.NotNull(connection.SendServerId);
                        Assert.NotNull(connection.StoredServerId);
                    }
                }
            }
        }

        public class WithAuth : RavenDB_3672
        {

            private const string apiKey = "test/ThisIsMySecret";

            protected override void ConfigureServer(RavenDbServer server, string fileSystemName)
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
                },
                }), new RavenJObject());
            }

            [Theory]
            [PropertyData("Storages")]
            public async Task SynchronizationTopologyDiscovererSimpleTestWithOAuth(string storage)
            {
                using (var store1 = NewStore(0, requestedStorage: storage, enableAuthentication: true, apiKey: apiKey))
                using (var store2 = NewStore(1, requestedStorage: storage, enableAuthentication: true, apiKey: apiKey))
                using (var store3 = NewStore(2, requestedStorage: storage, enableAuthentication: true, apiKey: apiKey))
                using (var store4 = NewStore(3, requestedStorage: storage, enableAuthentication: true, apiKey: apiKey))
                using (var store5 = NewStore(4, requestedStorage: storage, enableAuthentication: true, apiKey: apiKey))
                {
                    await store1.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store2.AsyncFilesCommands.ToSynchronizationDestination());
                    await store2.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store3.AsyncFilesCommands.ToSynchronizationDestination());
                    await store3.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store4.AsyncFilesCommands.ToSynchronizationDestination());
                    await store4.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store5.AsyncFilesCommands.ToSynchronizationDestination());
                    await store5.AsyncFilesCommands.Synchronization.SetDestinationsAsync(store1.AsyncFilesCommands.ToSynchronizationDestination());

                    using (var session = store1.OpenAsyncSession())
                    {
                        session.RegisterUpload("file1.bin", new MemoryStream());
                        await session.SaveChangesAsync();
                    }

                    await store1.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store2.AsyncFilesCommands, "file1.bin");

                    await store2.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store3.AsyncFilesCommands, "file1.bin");

                    await store3.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store4.AsyncFilesCommands, "file1.bin");

                    await store4.AsyncFilesCommands.Synchronization.StartAsync();
                    WaitForFile(store5.AsyncFilesCommands, "file1.bin");

                    await store5.AsyncFilesCommands.Synchronization.StartAsync();

                    var metadata = await store5.AsyncFilesCommands.GetMetadataForAsync("file1.bin");

                    Assert.NotNull(metadata);

                    var url = store1.Url.ForFilesystem(store1.DefaultFileSystem) + "/admin/synchronization/topology/view";

                    var request = store1
                        .JsonRequestFactory
                        .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Post, store1.AsyncFilesCommands.PrimaryCredentials, store1.Conventions));

                    var json = (RavenJObject)request.ReadResponseJson();
                    var topology = json.Deserialize<SynchronizationTopology>(new DocumentConvention());

                    Assert.NotNull(topology);
                    Assert.Equal(5, topology.Servers.Count);
                    Assert.Equal(5, topology.Connections.Count);

                    topology.Connections.Single(x => x.Destination == store1.Url.ForFilesystem(store1.DefaultFileSystem) && x.Source == store5.Url.ForFilesystem(store5.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store2.Url.ForFilesystem(store2.DefaultFileSystem) && x.Source == store1.Url.ForFilesystem(store1.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store3.Url.ForFilesystem(store3.DefaultFileSystem) && x.Source == store2.Url.ForFilesystem(store2.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store4.Url.ForFilesystem(store4.DefaultFileSystem) && x.Source == store3.Url.ForFilesystem(store3.DefaultFileSystem));
                    topology.Connections.Single(x => x.Destination == store5.Url.ForFilesystem(store5.DefaultFileSystem) && x.Source == store4.Url.ForFilesystem(store4.DefaultFileSystem));

                    foreach (var connection in topology.Connections.Where(x => x.Destination != store1.Url.ForFilesystem(store1.DefaultFileSystem) && x.Source != store5.Url.ForFilesystem(store5.DefaultFileSystem)))
                    {
                        Assert.Equal(ReplicatonNodeState.Online, connection.SourceToDestinationState);
                        Assert.Equal(ReplicatonNodeState.Online, connection.DestinationToSourceState);
                        Assert.NotNull(connection.Source);
                        Assert.NotNull(connection.Destination);
                        Assert.NotNull(connection.LastSourceFileEtag);
                        Assert.NotNull(connection.SendServerId);
                        Assert.NotNull(connection.StoredServerId);
                    }
                }
            }
        }
    }
}
 
