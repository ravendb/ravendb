using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_3974 : ReplicationBase
    {
        protected override void ConfigureConfig(InMemoryRavenConfiguration inMemoryRavenConfiguration)
        {
            inMemoryRavenConfiguration.Settings["Raven/Licensing/AllowAdminAnonymousAccessForCommercialUse"] = "true";
        }

        protected override void ConfigureDatabase(Database.DocumentDatabase database, string databaseName = null)
        {
            database.Documents.Put("Raven/ApiKeys/ReadWrite", null, RavenJObject.FromObject(new ApiKeyDefinition
            {
                Name = "ReadWrite",
                Secret = "JKaPAMUsASEifLuZNDeFXuUj5jy",
                Enabled = true,
                Databases = new List<ResourceAccess>
                {
                    new ResourceAccess {TenantId = "*"},
                    new ResourceAccess {TenantId = Constants.SystemDatabase},
                    new ResourceAccess {TenantId = databaseName, Admin = true}
                }
            }), new RavenJObject(), null);

            database.Documents.Put("Raven/ApiKeys/OnlyReadOnly", null, RavenJObject.FromObject(new ApiKeyDefinition
            {
                Name = "OnlyReadOnly",
                Secret = "JKaPAMUsASEifLuZNDeFXuUj5jy",
                Enabled = true,
                Databases = new List<ResourceAccess>
                {
                    new ResourceAccess {TenantId = Constants.SystemDatabase, Admin = false, ReadOnly = true},
                    new ResourceAccess {TenantId = databaseName, Admin = false, ReadOnly = true}
                }
            }), new RavenJObject(), null);
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_make_post_requests_when_apikey_is_readonly(string storageName)
        {
            Authentication.EnableOnce();
            using (var server = GetNewServer(requestedStorage: storageName, enableAuthentication: true, configureConfig: ConfigureConfig))
            {
                ConfigureDatabase(server.SystemDatabase);
                EnableAuthentication(server.SystemDatabase);
                using (var store = new DocumentStore() {Url = "http://localhost:8079", ApiKey = "ReadWrite/JKaPAMUsASEifLuZNDeFXuUj5jy" }.Initialize())
                {
                    using (var session = store.OpenSession())
                    {
                        for (var i = 0; i < 400; i++)
                            session.Store(new Test {Id = "Documents/" + i});
                        session.SaveChanges();
                    }
                }

                using (var store = new DocumentStore() { Url = "http://localhost:8079", ApiKey = "OnlyReadOnly/JKaPAMUsASEifLuZNDeFXuUj5jy" }.Initialize())
                {
                    using (var session = store.OpenSession())
                    {
                        var list = session.Load<object>(Enumerable.Range(1, 100).Select(i => "Documents/" + i));
                        Assert.Equal(100, list.Length);
                    }
                }
            }
        }

        private class Test
        {
            public string Id { get; set; }
        }
    }
}
