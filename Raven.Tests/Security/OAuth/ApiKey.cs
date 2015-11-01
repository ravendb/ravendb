using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Security.OAuth
{
    using Raven.Abstractions.Connection;

    public class ApiKey : RavenTest
    {
        private const string apiKey = "test/ThisIsMySecret";

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;


            Authentication.EnableOnce();
        }

        protected override void ModifyServer(Server.RavenDbServer ravenDbServer)
        {
            ravenDbServer.SystemDatabase.Documents.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
            {
                Name = "test",
                Secret = "ThisIsMySecret",
                Enabled = true,
                Databases = new List<ResourceAccess>
                {
                    new ResourceAccess{TenantId = "*"}, 
                    new ResourceAccess{TenantId = Constants.SystemDatabase}, 
                }
            }), new RavenJObject(), null);
        }

        protected override void ModifyStore(DocumentStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
            store.Credentials = null;
            store.ApiKey = apiKey;
        }

        [Fact]
        public void TestApiKeyStoreAndLoad()
        {
            const string id = "test/1";
            const string name = "My name";

            using (var store = NewRemoteDocumentStore(enableAuthentication: true))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestClass { Name = name, Id = id });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(name, session.Load<TestClass>(id).Name);
                }
            }
        }

        [Fact]
        public void CanAuthAsAdminAgainstTenantDb()
        {
            using (var server = GetNewServer(enableAuthentication: true))
            {

                server.SystemDatabase.Documents.Put("Raven/ApiKeys/sysadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = "sysadmin",
                    Secret = "ThisIsMySecret",
                    Enabled = true,
                    Databases = new List<ResourceAccess>
                {
                    new ResourceAccess{TenantId = Constants.SystemDatabase, Admin = true}, 
                }
                }), new RavenJObject(), null);

                server.SystemDatabase.Documents.Put("Raven/ApiKeys/dbadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = "dbadmin",
                    Secret = "ThisIsMySecret",
                    Enabled = true,
                    Databases = new List<ResourceAccess>
                {
                    new ResourceAccess{TenantId = "*", Admin = true}, 
                    new ResourceAccess{TenantId = Constants.SystemDatabase, Admin = false}, 
                }
                }), new RavenJObject(), null);

                var serverUrl = server.SystemDatabase.ServerUrl;
                using (var store = new DocumentStore
                {
                    Url = serverUrl,
                    ApiKey = "sysadmin/ThisIsMySecret",
                    Conventions = {FailoverBehavior = FailoverBehavior.FailImmediately}
                }.Initialize())
                {
                    store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("test");
                }

                using (var store = new DocumentStore
                {
                    Url = serverUrl,
                    ApiKey = "dbadmin/ThisIsMySecret"
                }.Initialize())
                {
                    store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store.Url + "/databases/test/admin/changeDbId", HttpMethods.Post, new OperationCredentials("dbadmin/ThisIsMySecret", null), store.Conventions))
                        .ExecuteRequest();// can do admin stuff

                    var httpJsonRequest = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store.Url + "/databases/test/debug/user-info", HttpMethods.Get, new OperationCredentials("dbadmin/ThisIsMySecret", null), store.Conventions));

                    var json = (RavenJObject)httpJsonRequest.ReadResponseJson();

                    Assert.True(json.Value<bool>("IsAdminCurrentDb"));
                }
            }
        }

        [Fact]
        public void CanAuthAsAdminAgainstTenantDbUsingLazyOperations()
        {
            using (var server = GetNewServer(enableAuthentication: true))
            {

                server.SystemDatabase.Documents.Put("Raven/ApiKeys/sysadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = "sysadmin",
                    Secret = "ThisIsMySecret",
                    Enabled = true,
                    Databases = new List<ResourceAccess>
                {
                    new ResourceAccess{TenantId = Constants.SystemDatabase, Admin = true}, 
                }
                }), new RavenJObject(), null);

                server.SystemDatabase.Documents.Put("Raven/ApiKeys/dbadmin", null, RavenJObject.FromObject(new ApiKeyDefinition
                {
                    Name = "dbadmin",
                    Secret = "ThisIsMySecret",
                    Enabled = true,
                    Databases = new List<ResourceAccess>
                {
                    new ResourceAccess{TenantId = "test", Admin = true}, 
                }
                }), new RavenJObject(), null);

                var serverUrl = server.SystemDatabase.ServerUrl;
                using (var store = new DocumentStore
                {
                    Url = serverUrl,
                    ApiKey = "sysadmin/ThisIsMySecret",
                    Conventions = { FailoverBehavior = FailoverBehavior.FailImmediately }
                }.Initialize())
                {
                    store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("test");
                }

                using (var store = new DocumentStore
                {
                    Url = serverUrl,
                    ApiKey = "dbadmin/ThisIsMySecret"
                }.Initialize())
                {
                    using (var x = store.OpenSession("test"))
                    {
                        x.Advanced.Lazily.Load<dynamic>("users/1");
                        x.Advanced.Lazily.Load<dynamic>("users/2");
                        x.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    }
                }
            }
        }

        class TestClass
        {
            public string Name { get; set; }
            public string Id { get; set; }
        }
    }
}
