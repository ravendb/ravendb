using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14464 : ClusterTestBase
    {
        public RavenDB_14464(ITestOutputHelper output) : base(output)
        {
        }

        private class Company_ByName : AbstractIndexCreationTask<Company>
        {
            public Company_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };
            }
        }

        [Fact]
        public async Task CanUseChangesApiAndToggleServer()
        {
            var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Conventions.OperationStatusFetchMode = OperationStatusFetchMode.ChangesApi;
                    documentStore.OnFailedRequest += (sender, args) => { };
                },
                Server = server,
                RunInMemory = false
            }))
            {
                new Company_ByName().Execute(store);
                put_1500_companies(store);

                WaitForIndexing(store);

                var task = Task.Run(async () =>
                {
                    var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = $"FROM INDEX '{new Company_ByName().IndexName}'" }, new QueryOperationOptions
                    {
                        MaxOpsPerSecond = 1
                    }));
                    var serverToggled = false;

                    while (store.WasDisposed == false)
                    {
                        try
                        {
                            if (serverToggled)
                            {
                                await operation.WaitForCompletionAsync();
                            }
                            else
                            {
                                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(1));
                            }
                            break;
                        }
                        catch (Exception e)
                        {
                            if (serverToggled == false)
                            {
                                Assert.True(e.GetType() == typeof(TimeoutException), "e.GetType() == typeof(TimeoutException)");
                                Assert.True(e.Message.Contains("did not get a reply for operation"), "e.Message.Contains('did not get a reply for operation')");
                                while (server.Disposed == false)
                                {
                                    await Task.Delay(500);
                                }
                                serverToggled = true;
                            }

                            Assert.True(e.GetType() == typeof(InvalidOperationException), "e.GetType() == typeof(InvalidOperationException)");
                            Assert.True(e.Message.StartsWith("Could not fetch state of operation"), "e.Message.StartsWith('Could not fetch state of operation')");
                            await Task.Delay(1000);
                        }
                    }
                });

                await Task.Delay(1000);
                server = await ToggleServer(server);
                await Task.Delay(5000);
                server = await ToggleServer(server);
                await Task.Delay(5000);
            }
        }

        private static void put_1500_companies(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 1500; i++)
                {
                    session.Store(new Company { Name = $"Company {i}" });
                }

                session.SaveChanges();
            }
        }

        private async Task<RavenServer> ToggleServer(RavenServer node)
        {
            if (node.Disposed)
            {
                var settings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                    [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "3",
                    [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "10",
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = node.WebUrl,
                    [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = node.Configuration.Cluster.ElectionTimeout.AsTimeSpan.TotalMilliseconds.ToString()
                };

                var dataDir = node.Configuration.Core.DataDirectory.FullPath.Split('/').Last();

                node = base.GetNewServer(new ServerCreationOptions() { DeletePrevious = false, RunInMemory = false, CustomSettings = settings, PartialPath = dataDir });
            }
            else
            {
                var nodeInfo = await DisposeServerAndWaitForFinishOfDisposalAsync(node);
            }

            return node;
        }
    }
}
