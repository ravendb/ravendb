using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Commercial;
using Raven.Server.Commercial.SetupWizard;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tools;

public class SetupUnsecuredClusterUsingRvn : ClusterTestBase
{
    public SetupUnsecuredClusterUsingRvn(ITestOutputHelper output) : base(output)
    {
    }
    [Fact]
    public async Task Should_Create_Unsecured_Cluster_And_Setup_Zip_File_From_Rvn_One_Node()
    {
        DoNotReuseServer();

        var unsecuredSetupInfo = new UnsecuredSetupInfo
        {
            Environment = StudioConfiguration.StudioEnvironment.Testing,
            ZipOnly = false,
            NodeSetupInfos = new Dictionary<string,NodeInfo>()
            {
                ["A"] = new() { Port = GetAvailablePort(), TcpPort = GetAvailablePort(), Addresses = new List<string> { "127.0.0.1" } },
            }
        };
      
        Assert.True(unsecuredSetupInfo.ZipOnly == false, nameof(unsecuredSetupInfo.ZipOnly) + " != false");
        
        var zipBytes = await UnsecuredSetupUtils.Setup(unsecuredSetupInfo,  new SetupProgressAndResult(tuple =>
        {
            if (tuple.Message != null)
            {
                Console.WriteLine(tuple.Message);
            }

            if (tuple.Exception != null)
            {
                Console.Error.WriteLine(tuple.Exception.Message);
            }
        }), CancellationToken.None);

        var settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(zipBytes, "A",
            new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None),
            out _,
            out _,
            out _,
            out _,
            out var otherNodesUrls,
            out _,
            false);

        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);

        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = serverUrl,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
            }
        });


        var dbName = GetDatabaseName();
        using (var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
        }.Initialize())
        {
            DatabaseRecord databaseRecord = new(dbName);
            CreateDatabaseOperation createDatabaseOperation = new(databaseRecord);
            store.Maintenance.Server.Send(createDatabaseOperation);
            using (var session = store.OpenAsyncSession(dbName))
            {
                var user = new User();
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }
            
            Assert.True(await WaitForValueAsync(() => server.ServerStore.GetClusterTopology().Members.Count == 1, true));
        }
    }
    
    [Fact]
    public async Task Should_Create_Unsecured_Cluster_And_Setup_Zip_File_From_Rvn_Three_Nodes()
    {
        DoNotReuseServer();

        var unsecuredSetupInfo = new UnsecuredSetupInfo
        {
            Environment = StudioConfiguration.StudioEnvironment.Testing,
            ZipOnly = false,
            NodeSetupInfos = new Dictionary<string,NodeInfo>()
            {
                ["A"] = new() { Port = GetAvailablePort(), TcpPort = GetAvailablePort(), Addresses = new List<string> { "127.0.0.1" } },
                ["B"] = new() { Port = GetAvailablePort(), TcpPort = GetAvailablePort(), Addresses = new List<string> { "127.0.0.1" } },
                ["C"] = new() { Port = GetAvailablePort(), TcpPort = GetAvailablePort(), Addresses = new List<string> { "127.0.0.1" } }
            }
        };
        Assert.True(unsecuredSetupInfo.ZipOnly == false, nameof(unsecuredSetupInfo.ZipOnly) + " != false");
        var zipBytes = await UnsecuredSetupUtils.Setup(unsecuredSetupInfo,  new SetupProgressAndResult(tuple =>
        {
            if (tuple.Message != null)
            {
                Console.WriteLine(tuple.Message);
            }

            if (tuple.Exception != null)
            {
                Console.Error.WriteLine(tuple.Exception.Message);
            }
        }), CancellationToken.None);

        var settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(zipBytes, "A",
            new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None),
            out _,
            out _,
            out _,
            out _,
            out var otherNodesUrls,
            out _,
            false);

        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);


        var url2 = otherNodesUrls["B"];
        var url3 = otherNodesUrls["C"];
        const int numberOfExpectedNodes = 3;

        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = serverUrl,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
            }
        });

        using var __ = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url2,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
            }
        });

        using var ___ = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url3,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
            }
        });

        var dbName = GetDatabaseName();
        using (var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
        }.Initialize())
        {
            {
                DatabaseRecord databaseRecord = new(dbName);
                CreateDatabaseOperation createDatabaseOperation = new(databaseRecord);
                store.Maintenance.Server.Send(createDatabaseOperation);
                var requestExecutor = store.GetRequestExecutor(dbName);
                using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
                {
                    await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(url2), ctx);
                    await requestExecutor.ExecuteAsync(new AddClusterNodeCommand(url3), ctx);
                }

                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(dbName, "B"));
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(dbName, "C"));
            }
            using (var store1 = new DocumentStore
            {
                Urls = new[] { serverUrl },
                Database = dbName
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] { url2 },
                Database = dbName
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                Urls = new[] { url3 },
                Database = dbName
            }.Initialize())
            {
                string userId;
                using (var session = store2.OpenAsyncSession())
                {
                    var user = new User();
                    await session.StoreAsync(user);
                    userId = user.Id;
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    await session.SaveChangesAsync();
                }

                using (var session = store1.OpenAsyncSession())
                {
                    Assert.NotNull(await session.LoadAsync<User>(userId));
                }

                using (var session = store3.OpenAsyncSession())
                {
                    Assert.NotNull(await session.LoadAsync<User>(userId));
                }
            }
            Assert.True(await WaitForValueAsync(() => server.ServerStore.GetClusterTopology().Members.Count == numberOfExpectedNodes, true));
        }
    }
}
