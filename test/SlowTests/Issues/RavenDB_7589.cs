using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7589 : RavenTestBase
    {
        private int _numberOfDatabasesRemoved;

        [Fact]
        public async Task CanImportIdentities()
        {
            DoNotReuseServer();

            var mre = new ManualResetEventSlim();

            Server.ServerStore.Cluster.DatabaseChanged += (sender, tuple) =>
            {
                if (tuple.Type != nameof(RemoveNodeFromDatabaseCommand))
                    return;

                var value = Interlocked.Increment(ref _numberOfDatabasesRemoved);

                if (value == 3)
                    mre.Set();
            };

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

                var identities = store.Maintenance.Send(new GetIdentitiesOperation());

                Assert.Equal(1502, identities.Count);
                Assert.Equal(2, identities["companies|"]);
                Assert.Equal(1, identities["addresses|"]);

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile1);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            string dbName2;
            using (var store = GetDocumentStore())
            {
                dbName2 = store.Database;

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile1);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var identities = store.Maintenance.Send(new GetIdentitiesOperation());

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

                operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile2);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            string dbName3;
            using (var store = GetDocumentStore())
            {
                dbName3 = store.Database;

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile1);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile2);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                var identities = store.Maintenance.Send(new GetIdentitiesOperation());

                Assert.Equal(1502, identities.Count);
                Assert.Equal(3, identities["companies|"]);
                Assert.Equal(2, identities["addresses|"]);
            }

            Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal(0, Server.ServerStore.Cluster.ReadIdentities(context, dbName1, 0, int.MaxValue).Count());
                Assert.Equal(0, Server.ServerStore.Cluster.ReadIdentities(context, dbName2, 0, int.MaxValue).Count());
                Assert.Equal(0, Server.ServerStore.Cluster.ReadIdentities(context, dbName3, 0, int.MaxValue).Count());
            }
        }
    }
}
