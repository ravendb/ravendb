using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7589 : RavenTestBase
    {
        [Fact]
        public async Task CanImportIdentities()
        {
            var path = NewDataPath(forceCreateDir: true);
            var exportFile1 = Path.Combine(path, "export1.ravendbdump");
            var exportFile2 = Path.Combine(path, "export2.ravendbdump");

            string dbName1;
            using (var store = GetDocumentStore())
            {
                dbName1 = store.Database;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Company 1"
                    }, "companies|");

                    session.Store(new Company
                    {
                        Name = "Company 2"
                    }, "companies|");

                    session.Store(new Address
                    {
                        City = "Torun"
                    }, "addresses|");

                    for (int i = 0; i < 1500; i++)
                    {
                        session.Store(new User
                        {
                            Name = $"U{i}"
                        }, $"users{i}|");
                    }

                    session.SaveChanges();
                }

                var identities = GetIdentities(store, 0, 2000);

                Assert.Equal(1502, identities.Count);
                Assert.Equal(2, identities["companies|"]);
                Assert.Equal(1, identities["addresses|"]);

                identities = GetIdentities(store, 0, 1);
                Assert.Equal(1, identities.Count);
                Assert.Equal(1, identities["addresses|"]);

                identities = GetIdentities(store, 1, 1);
                Assert.Equal(1, identities.Count);
                Assert.Equal(2, identities["companies|"]);

                identities = GetIdentities(store, 1502, 10);
                Assert.Equal(0, identities.Count);

                await store.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), exportFile1);
            }

            string dbName2;
            using (var store = GetDocumentStore())
            {
                dbName2 = store.Database;

                await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), exportFile1);

                var identities = GetIdentities(store, 0, 2000);

                Assert.Equal(1502, identities.Count);
                Assert.Equal(2, identities["companies|"]);
                Assert.Equal(1, identities["addresses|"]);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Company 3"
                    }, "companies|");

                    session.Store(new Address
                    {
                        City = "Bydgoszcz"
                    }, "addresses|");

                    session.SaveChanges();
                }

                await store.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), exportFile2);
            }

            string dbName3;
            using (var store = GetDocumentStore())
            {
                dbName3 = store.Database;

                await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), exportFile1);
                await store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), exportFile2);

                var identities = GetIdentities(store, 0, 2000);

                Assert.Equal(1502, identities.Count);
                Assert.Equal(3, identities["companies|"]);
                Assert.Equal(2, identities["addresses|"]);
            }

            var mre = new ManualResetEventSlim();

            Server.ServerStore.Cluster.DatabaseChanged += (sender, tuple) => mre.Set();

            Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal(0, Server.ServerStore.Cluster.ReadIdentities(context, dbName1, 0, int.MaxValue).Count());
                Assert.Equal(0, Server.ServerStore.Cluster.ReadIdentities(context, dbName2, 0, int.MaxValue).Count());
                Assert.Equal(0, Server.ServerStore.Cluster.ReadIdentities(context, dbName3, 0, int.MaxValue).Count());
            }
        }

        private static Dictionary<string, long> GetIdentities(IDocumentStore store, int start, int pageSize)
        {
            using (var commands = store.Commands())
            {
                var command = new GetIdentitiesCommand(start, pageSize);

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        private class GetIdentitiesCommand : RavenCommand<Dictionary<string, long>>
        {
            private readonly int _start;
            private readonly int _pageSize;
            public override bool IsReadRequest { get; } = true;

            public GetIdentitiesCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;

                CanCache = false;
                CanCacheAggressively = false;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/debug/identities?start={_start}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                Result = new Dictionary<string, long>();

                foreach (var propertyName in response.GetPropertyNames())
                {
                    Result[propertyName] = (long)response[propertyName];
                }
            }
        }
    }
}
