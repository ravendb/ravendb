using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands.Cluster;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Commercial;
using Raven.Server.Commercial.SetupWizard;
using Raven.Server.Config;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tools;

public class SetupSecuredClusterUsingRvn : ClusterTestBase
{
    public SetupSecuredClusterUsingRvn(ITestOutputHelper output) : base(output)
    {
    }

    [LicenseRequiredFact]
    public async Task Should_Create_Secured_Cluster_Generating_Self_Singed_Cert_And_Setup_Zip_File_From_Rvn_Three_Nodes()
    {
        DoNotReuseServer();

        var license = Environment.GetEnvironmentVariable("RAVEN_LICENSE");
        Assert.True(license != null, nameof(license) + " != null");
        var licenseObj = JsonConvert.DeserializeObject<License>(license);

        byte[] selfSignedTestCertificate = CertificateUtils.CreateSelfSignedTestCertificate("localhost", "RavenTestsServer");

        var setupInfo = new SetupInfo
        {
            Domain = "localhost",
            Email = "suppport@ravendb.net",
            RootDomain = "development.run",
            Password = null,
            Certificate = Convert.ToBase64String(selfSignedTestCertificate),
            LocalNodeTag = "A",
            Environment = StudioConfiguration.StudioEnvironment.None,
            License = licenseObj,
            NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
            {
                ["A"] = new() { Port = 443, TcpPort = 38887, Addresses = new List<string> { "127.0.0.1" } },
                ["B"] = new() { Port = 448, TcpPort = 38888, Addresses = new List<string> { "127.0.0.1" } },
                ["C"] = new() { Port = 446, TcpPort = 38889, Addresses = new List<string> { "127.0.0.1" } }
            }
        };

        Assert.True(setupInfo.ModifyLocalServer == false, nameof(setupInfo.ModifyLocalServer) + " != false");

        var zipBytes = await OwnCertificateSetupUtils.Setup(setupInfo, new SetupProgressAndResult(tuple =>
        {
            if (tuple.Message != null)
            {
                Output.WriteLine(tuple.Message);
            }
            if (tuple.Exception != null)
            {
                Output.WriteLine(tuple.Exception.Message);
            }
        }), CancellationToken.None);


        var settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(zipBytes, "A",
            new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None),
            out var serverCertBytes,
            out var serverCert,
            out var clientCert,
            out _,
            out var otherNodesUrls,
            out _);

        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string url1);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string _);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);


        var tempFileName = GetTempFileName();
        await File.WriteAllBytesAsync(tempFileName, serverCertBytes);
        var url2 = otherNodesUrls["B"];
        var url3 = otherNodesUrls["C"];
        const int numberOfExpectedNodes = 3;

        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url1,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url1,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
            }
        });

        using var __ = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url2,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url2,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
            }
        });

        using var ___ = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url3,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url3,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
            }
        });

        var dbName = GetDatabaseName();
        using (var store = new DocumentStore
        {
            Urls = new[] { url1 },
            Certificate = serverCert,
        }.Initialize())
        {
            {
                DatabaseRecord databaseRecord = new(dbName);
                CreateDatabaseOperation createDatabaseOperation = new(databaseRecord);
                store.Maintenance.Server.Send(createDatabaseOperation);
                await store.Maintenance.Server.SendAsync(new PutClientCertificateOperation("client certificate", clientCert, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin));
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
                Urls = new[] { url1 },
                Certificate = clientCert,
                Database = dbName
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] { url2 },
                Certificate = clientCert,
                Database = dbName
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                Urls = new[] { url3 },
                Certificate = clientCert,
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

    [LicenseRequiredFact]
    public async Task Should_Create_Secured_Cluster_Generating_Self_Singed_Cert_And_Setup_Zip_File_From_Rvn_One_Node()
    {
        DoNotReuseServer();

        var license = Environment.GetEnvironmentVariable("RAVEN_LICENSE");
        Assert.True(license != null, nameof(license) + " != null");
        var licenseObj = JsonConvert.DeserializeObject<License>(license);

        byte[] selfSignedTestCertificate = CertificateUtils.CreateSelfSignedTestCertificate("localhost", "RavenTestsServer");

        var setupInfo = new SetupInfo
        {
            Domain = "localhost",
            Email = "suppport@ravendb.net",
            RootDomain = "development.run",
            Password = null,
            Certificate = Convert.ToBase64String(selfSignedTestCertificate),
            LocalNodeTag = "A",
            Environment = StudioConfiguration.StudioEnvironment.None,
            License = licenseObj,
            NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>()
            {
                ["A"] = new() { Port = 443, TcpPort = 38887, Addresses = new List<string> { "127.0.0.1" } },
            }
        };

        Assert.True(setupInfo.ModifyLocalServer == false, nameof(setupInfo.ModifyLocalServer) + " != false");


        var zipBytes = await OwnCertificateSetupUtils.Setup(setupInfo, new SetupProgressAndResult(tuple =>
        {
            if (tuple.Message != null)
            {
                Output.WriteLine(tuple.Message);
            }
            if (tuple.Exception != null)
            {
                Output.WriteLine(tuple.Exception.Message);
            }
        }), CancellationToken.None);


        var settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(zipBytes, "A",
            new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None),
            out var serverCertBytes,
            out var serverCert,
            out var clientCert,
            out _,
            out _,
            out _);

        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string url1);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string _);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);


        var tempFileName = GetTempFileName();
        await File.WriteAllBytesAsync(tempFileName, serverCertBytes);
        const int numberOfExpectedNodes = 1;

        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url1,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url1,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
            }
        });

        var dbName = GetDatabaseName();
        using (var store = new DocumentStore
        {
            Urls = new[] { url1 },
            Certificate = serverCert,
        }.Initialize())
        {
            {
                DatabaseRecord databaseRecord = new(dbName);
                CreateDatabaseOperation createDatabaseOperation = new(databaseRecord);
                store.Maintenance.Server.Send(createDatabaseOperation);
                await store.Maintenance.Server.SendAsync(new PutClientCertificateOperation("client certificate", clientCert, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin));
            }
            using (var store1 = new DocumentStore
            {
                Urls = new[] { url1 },
                Certificate = clientCert,
                Database = dbName
            }.Initialize())

                Assert.True(await WaitForValueAsync(() => server.ServerStore.GetClusterTopology().Members.Count == numberOfExpectedNodes, true));
        }

    }

    [LicenseRequiredFact]
    public async Task Should_Create_Secured_Cluster_From_Rvn_Using_Lets_Encrypt_Mode_One_Node()
    {
        DoNotReuseServer();

        var license = Environment.GetEnvironmentVariable("RAVEN_LICENSE");
        Assert.True(license != null, nameof(license) + " != null");
        var licenseObj = JsonConvert.DeserializeObject<License>(license);

        const string domain = "rvnTest";
        const string rootDomain = "development.run";
        const string publicTcpServerUrl1 = $"tcp://a.{domain}.{rootDomain}:38879";
        const string tcpServerUrl1 = "tcp://127.0.0.1:38879";


        var setupInfo = new SetupInfo
        {
            Environment = StudioConfiguration.StudioEnvironment.None,
            License = licenseObj,
            Email = "support@ravendb.net",
            Domain = domain,
            RootDomain = rootDomain,
            LocalNodeTag = "A",
            NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>
            {
                {"A", new SetupInfo.NodeInfo {Port = 443, TcpPort = 38879, Addresses = new List<string> {"127.0.0.1"}}},
            }
        };

        Assert.True(setupInfo.ModifyLocalServer == false, nameof(setupInfo.ModifyLocalServer) + " != false");

        var zipBytes = await LetsEncryptSetupUtils.Setup(setupInfo, new SetupProgressAndResult(tuple =>
            {
                if (tuple.Message != null)
                {
                    Output.WriteLine(tuple.Message);
                }

                if (tuple.Exception != null)
                {
                    Output.WriteLine(tuple.Exception.Message);
                }
            })
        {
            Processed = 0,
            Total = 4
        },
            CancellationToken.None);

        X509Certificate2 serverCert;
        X509Certificate2 clientCert;
        byte[] serverCertBytes;
        BlittableJsonReaderObject settingsJsonObject;
        try
        {
            settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(
                zipBytes,
                "A",
                new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None),
                out serverCertBytes,
                out serverCert,
                out clientCert,
                out var _,
                out _,
                out var _);

        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
        }

        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string url1);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl1);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

        var tempFileName = GetTempFileName();
        await File.WriteAllBytesAsync(tempFileName, serverCertBytes);


        const int numberOfExpectedNodes = 1;

        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url1,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl1,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                [RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] = tcpServerUrl1,
                [RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] = publicTcpServerUrl1,
            }

        });

        var dbName = GetDatabaseName();

        using (var store = new DocumentStore
        {
            Urls = new[] { url1 },
            Certificate = serverCert
        }.Initialize())
        {
            DatabaseRecord databaseRecord = new(dbName);
            CreateDatabaseOperation createDatabaseOperation = new(databaseRecord);
            store.Maintenance.Server.Send(createDatabaseOperation);
            await store.Maintenance.Server.SendAsync(new PutClientCertificateOperation("client certificate", clientCert, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin));

        }

        using (var store1 = new DocumentStore
        {
            Urls = new[] { url1 },
            Certificate = clientCert,
            Database = dbName
        }.Initialize())

            Assert.True(await WaitForValueAsync(() => server.ServerStore.GetClusterTopology().Members.Count == numberOfExpectedNodes, true));
    }

    [LicenseRequiredFact]
    public async Task Should_Create_Secured_Cluster_From_Rvn_Using_Lets_Encrypt_Mode_Three_Nodes()
    {
        DoNotReuseServer();

        var license = Environment.GetEnvironmentVariable("RAVEN_LICENSE");
        Assert.True(license != null, nameof(license) + " != null");
        var licenseObj = JsonConvert.DeserializeObject<License>(license);

        const string domain = "rvnTest";
        const string rootDomain = "development.run";
        const string serverUrl2 = "https://127.0.0.1:444";
        const string serverUrl3 = "https://127.0.0.1:446";
        const string publicTcpServerUrl1 = $"tcp://a.{domain}.{rootDomain}:38879";
        const string publicTcpServerUrl2 = $"tcp://b.{domain}.{rootDomain}:38880";
        const string publicTcpServerUrl3 = $"tcp://c.{domain}.{rootDomain}:38888";
        const string tcpServerUrl1 = "tcp://127.0.0.1:38879";
        const string tcpServerUrl2 = "tcp://127.0.0.1:38880";
        const string tcpServerUrl3 = "tcp://127.0.0.1:38888";


        var setupInfo = new SetupInfo
        {
            Environment = StudioConfiguration.StudioEnvironment.None,
            License = licenseObj,
            Email = "support@ravendb.net",
            Domain = domain,
            RootDomain = rootDomain,
            LocalNodeTag = "A",
            NodeSetupInfos = new Dictionary<string, SetupInfo.NodeInfo>
            {
                {"A", new SetupInfo.NodeInfo {Port = 443, TcpPort = 38879, Addresses = new List<string> {"127.0.0.1"}}},
                {"B", new SetupInfo.NodeInfo {Port = 444, TcpPort = 38880, Addresses = new List<string> {"127.0.0.1"}}},
                {"C", new SetupInfo.NodeInfo {Port = 446, TcpPort = 38888, Addresses = new List<string> {"127.0.0.1"}}}
            }
        };
        Assert.True(setupInfo.ModifyLocalServer == false, nameof(setupInfo.ModifyLocalServer) + " != false");

        var zipBytes = await LetsEncryptSetupUtils.Setup(setupInfo, new SetupProgressAndResult(tuple =>
            {
                if (tuple.Message != null)
                {
                    Output.WriteLine(tuple.Message);
                }

                if (tuple.Exception != null)
                {
                    Output.WriteLine(tuple.Exception.Message);
                }
            })
        {
            Processed = 0,
            Total = 4
        },
            CancellationToken.None);

        X509Certificate2 serverCert;
        X509Certificate2 clientCert;
        byte[] serverCertBytes;
        BlittableJsonReaderObject settingsJsonObject;
        Dictionary<string, string> otherNodesUrls;
        try
        {
            settingsJsonObject = SetupManager.ExtractCertificatesAndSettingsJsonFromZip(
                zipBytes,
                "A",
                new JsonOperationContext(1024, 1024 * 4, 32 * 1024, SharedMultipleUseFlag.None),
                out serverCertBytes,
                out serverCert,
                out clientCert,
                out var _,
                out otherNodesUrls,
                out var _);

        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Unable to extract setup information from the zip file.", e);
        }

        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificatePassword), out string certPassword);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail), out string letsEncryptEmail);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), out string url1);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ServerUrls), out string serverUrl1);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.SetupMode), out SetupMode setupMode);
        settingsJsonObject.TryGet(RavenConfiguration.GetKey(x => x.Core.ExternalIp), out string externalIp);

        var tempFileName = GetTempFileName();
        await File.WriteAllBytesAsync(tempFileName, serverCertBytes);

        var url2 = otherNodesUrls["B"];
        var url3 = otherNodesUrls["C"];


        const int numberOfExpectedNodes = 3;

        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url1,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl1,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                [RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] = tcpServerUrl1,
                [RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] = publicTcpServerUrl1,
            }

        });

        using var __ = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url2,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl2,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                [RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] = tcpServerUrl2,
                [RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] = publicTcpServerUrl2
            }
        });

        using var ___ = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new ConcurrentDictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = tempFileName,
                [RavenConfiguration.GetKey(x => x.Security.CertificateLetsEncryptEmail)] = letsEncryptEmail,
                [RavenConfiguration.GetKey(x => x.Security.CertificatePassword)] = certPassword,
                [RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)] = url3,
                [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl3,
                [RavenConfiguration.GetKey(x => x.Core.SetupMode)] = setupMode.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ExternalIp)] = externalIp,
                [RavenConfiguration.GetKey(x => x.Core.TcpServerUrls)] = tcpServerUrl3,
                [RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)] = publicTcpServerUrl3
            }
        });

        var dbName = GetDatabaseName();

        using (var store = new DocumentStore
        {
            Urls = new[] { url1 },
            Certificate = serverCert
        }.Initialize())
        {
            DatabaseRecord databaseRecord = new(dbName);
            CreateDatabaseOperation createDatabaseOperation = new(databaseRecord);
            store.Maintenance.Server.Send(createDatabaseOperation);
            await store.Maintenance.Server.SendAsync(new PutClientCertificateOperation("client certificate", clientCert, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin));

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
            Urls = new[] { url1 },
            Certificate = clientCert,
            Database = dbName
        }.Initialize())
        using (var store2 = new DocumentStore
        {
            Urls = new[] { url2 },
            Certificate = clientCert,
            Database = dbName
        }.Initialize())
        using (var store3 = new DocumentStore
        {
            Urls = new[] { url3 },
            Certificate = clientCert,
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
