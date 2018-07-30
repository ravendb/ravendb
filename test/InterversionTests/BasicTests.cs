using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests
{
    public class BasicTests : InterversionTestBase
    {
        [Fact]
        public async Task Test()
        {
            var getStoreTask405 = GetDocumentStoreAsync("4.0.5");
            var getStoreTask406Patch = GetDocumentStoreAsync("4.0.6-patch-40047");

            await Task.WhenAll(getStoreTask405, getStoreTask406Patch);

            AssertStore(await getStoreTask405);
            AssertStore(await getStoreTask406Patch);
            AssertStore(GetDocumentStore());
        }

        [Fact]
        public async Task TestBasicReplicationInMixedCluster()
        {
            (var urlB, var serverB) = await GetServerAsync("4.0.6-patch-40047");
            (var urlC, var serverC) = await GetServerAsync("4.0.6-patch-40047");

            using (var storeA = GetDocumentStore(new Options
            {
                CreateDatabase = true,
                DeleteDatabaseOnDispose = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                await AddNodeToCluster(storeA, urlB.ToString());
                await Task.Delay(2000);
                await AddNodeToCluster(storeA, urlC.ToString());
                await Task.Delay(500);

                var dbName = "MixedClusterTestDB";
                var doc = new DatabaseRecord(dbName)
                {
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                        [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                        [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                    }
                };

                var databasePutResult = await storeA.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, 3));
                Assert.Equal(3, databasePutResult.NodesAddedTo.Count);

                using (var session = storeA.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }
                using (var storeB = await GetStore(urlB.ToString(), serverB, dbName, new InterversionTestOptions
                {
                    CreateDatabase = false,
                    ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
                }))
                using (var storeC = await GetStore(urlC.ToString(), serverC, dbName, new InterversionTestOptions
                {
                    CreateDatabase = false,
                    ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
                }))
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        "users/1",
                        u => u.Name.Equals("aviv"),
                        TimeSpan.FromSeconds(10), 
                        new List<DocumentStore>
                        {
                            storeA, storeB, storeC
                        },
                        dbName));

                    storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                    storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
                }                
            }
        }

        private static async Task AddNodeToCluster(DocumentStore store, string url)
        {
            var addNodeRequest = await store.GetRequestExecutor().HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Put, $"{store.Urls[0]}/admin/cluster/node?url={url}"));
            Assert.True(addNodeRequest.IsSuccessStatusCode);
        }

        private async Task<DocumentStore> GetStore(string serverUrl, Process serverProcess = null, [CallerMemberName] string database = null, InterversionTestOptions options = null)
        {
            options = options ?? InterversionTestOptions.Default;
            var name = database ?? GetDatabaseName(null);

            if (options.ModifyDatabaseName != null)
                name = options.ModifyDatabaseName(name) ?? name;

            var store = new DocumentStore
            {
                Urls = new[] { serverUrl },
                Database = name
            };

            options.ModifyDocumentStore?.Invoke(store);

            store.Initialize();

            if (options.CreateDatabase)
            {
                var dbs = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, 10));
                foreach (var db in dbs)
                {
                    if (db == name)
                    {
                        throw new InvalidOperationException($"Database '{name}' already exists.");
                    }
                }

                var doc = new DatabaseRecord(name)
                {
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                        [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                        [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = true.ToString(),
                        [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                    }
                };

                options.ModifyDatabaseRecord?.Invoke(doc);


                DatabasePutResult result;
                result = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, options.ReplicationFactor));
            }

            if (serverProcess != null)
            {
                store.AfterDispose += (sender, e) =>
                {
                    KillSlavedServerProcess(serverProcess);
                };
            }
            return store;
        }

        private static void AssertStore(IDocumentStore store)
        {
            using (store)
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c = session.Load<Company>("companies/1");
                    Assert.NotNull(c);
                    Assert.Equal("HR", c.Name);
                }
            }
        }
    }
}
