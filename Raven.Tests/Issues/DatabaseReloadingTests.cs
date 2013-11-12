// -----------------------------------------------------------------------
//  <copyright file="DatabaseReloadingTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Issues
{
    public class DatabaseReloadingTests : RavenTest
    {
        private const string TenantName = "MyTenant";

        [Fact]
        public void Should_save_put_to_tenant_database_if_tenant_database_is_reloaded_before_the_put_transaction()
        {
			using (var server = GetNewServer(runInMemory: false))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument { Id = TenantName, Settings = { {"Raven/DataDir", @"~\Databases\Mine"} }, });

                var tx1 = new TransactionInformation {Id = Guid.NewGuid().ToString()};
                var tx2 = new TransactionInformation { Id = Guid.NewGuid().ToString() };

                var tenantDatabaseDocument = store.DatabaseCommands.Get("Raven/Databases/" + TenantName);
                server.SystemDatabase.Put("Raven/Databases/" + TenantName, null, tenantDatabaseDocument.DataAsJson, tenantDatabaseDocument.Metadata, tx1);
                server.SystemDatabase.PrepareTransaction(tx1.Id);
				server.SystemDatabase.Commit(tx1.Id);

                var tenantDb = GetDocumentDatabaseForTenant(server, TenantName);
                tenantDb.Put("Foo/1", null, new RavenJObject { { "Test", "123" } }, new RavenJObject(), tx2);
                tenantDb.PrepareTransaction(tx2.Id);
				tenantDb.Commit(tx2.Id);

                var fooDoc = tenantDb.Get("Foo/1", new TransactionInformation {Id = Guid.NewGuid().ToString()});
                Assert.NotNull(fooDoc);
            }
        }

        [Fact]
        public void Should_save_put_to_tenant_database_if_tenant_database_is_reloaded_in_the_middle_of_the_put_transaction()
        {
			using (var server = GetNewServer(runInMemory: false))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument { Id = TenantName, Settings = { { "Raven/DataDir", @"~\Databases\Mine" } }, });

                var tx1 = new TransactionInformation { Id = Guid.NewGuid().ToString() };
                var tx2 = new TransactionInformation { Id = Guid.NewGuid().ToString() };

                var tenantDatabaseDocument = store.DatabaseCommands.Get("Raven/Databases/" + TenantName);
                server.SystemDatabase.Put("Raven/Databases/mydb", null, tenantDatabaseDocument.DataAsJson, tenantDatabaseDocument.Metadata, tx1);

                var tenantDb = GetDocumentDatabaseForTenant(server, TenantName);
                tenantDb.Put("Foo/1", null, new RavenJObject { { "Test", "123" } }, new RavenJObject(), tx2);

				server.SystemDatabase.PrepareTransaction(tx1.Id);
                server.SystemDatabase.Commit(tx1.Id);

                tenantDb = GetDocumentDatabaseForTenant(server, TenantName);
				tenantDb.PrepareTransaction(tx2.Id);
                tenantDb.Commit(tx2.Id);

                var fooDoc = tenantDb.Get("Foo/1", new TransactionInformation { Id = Guid.NewGuid().ToString() });
                Assert.NotNull(fooDoc);
            }
        }

        [Fact]
        public void Should_fail_put_to_tenant_database_if_tenant_database_is_reloaded_after_the_put_transaction_because_tx_was_reset()
        {
            using (var server = GetNewServer(runInMemory: false))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize())
            {
                store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument { Id = TenantName, Settings = { { "Raven/DataDir", @"~\Databases\Mine" } }, });

                var tx1 = new TransactionInformation { Id = Guid.NewGuid().ToString() };
                var tx2 = new TransactionInformation { Id = Guid.NewGuid().ToString() };

				var tenantDb = GetDocumentDatabaseForTenant(server, TenantName);
				tenantDb.Put("Foo/1", null, new RavenJObject { { "Test", "123" } }, new RavenJObject(), tx2);
				
				var tenantDatabaseDocument = store.DatabaseCommands.Get("Raven/Databases/" + TenantName);
                server.SystemDatabase.Put("Raven/Databases/" + TenantName, null, tenantDatabaseDocument.DataAsJson, tenantDatabaseDocument.Metadata, tx1);
				server.SystemDatabase.PrepareTransaction(tx1.Id);
				server.SystemDatabase.Commit(tx1.Id);

				tenantDb = GetDocumentDatabaseForTenant(server, TenantName);

				var exception = Assert.Throws<InvalidOperationException>(() => tenantDb.Commit(tx2.Id));
				Assert.Contains("There is no transaction with id:", exception.Message);
            }
        }

        private static DocumentDatabase GetDocumentDatabaseForTenant(RavenDbServer server, string databaseName)
        {
            var myDb = server.Server.GetDatabaseInternal(databaseName);
            myDb.Wait();
            return myDb.Result;
        }
    }
}