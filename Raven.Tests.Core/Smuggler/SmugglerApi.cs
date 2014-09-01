using Raven.Abstractions.Smuggler;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Raven.Tests.Core.Utils.Transformers;
using System.IO;
using Xunit;
using Raven.Abstractions.Data;
using Raven.Smuggler;

namespace Raven.Tests.Core.Smuggler
{
    public class SmugglerAPI : RavenCoreTestBase
    {
        public const int Port1 = 8081;
        public const int Port2 = 8082;
        public const string ServerName1 = "Raven.Tests.Core.Server";
        public const string ServerName2 = "Raven.Tests.Core.Server";

        [Fact]
        public void test()
        {
            using (var server1 = new RavenDbServer(new RavenConfiguration()
            {
                Port = Port1,
                ServerName = ServerName1
            })
            {
                RunInMemory = true,
                UseEmbeddedHttpServer = true
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument("db1");
                ((ServerClient)Server.DocumentStore.DatabaseCommands.ForSystemDatabase()).GlobalAdmin.CreateDatabase(doc);

                using (var store1 = new DocumentStore
                {
                    HttpMessageHandler = Server.DocumentStore.HttpMessageHandler,
                    Url = "http://localhost:8081",
                    DefaultDatabase = "db1"
                }.Initialize())
                {
                    new Users_ByName().Execute(store1);
                    new UsersTransformer().Execute(store1);

                    using (var session = store1.OpenSession())
                    {
                        session.Store(new User { Name = "Name1", LastName = "LastName1" });
                        session.Store(new User { Name = "Name2", LastName = "LastName2" });
                        session.SaveChanges();
                    }

                    store1.DatabaseCommands.PutAttachment("attachement1", null, new MemoryStream(new byte[] { 3 }), new RavenJObject());

                    using (var server2 = new RavenDbServer(new RavenConfiguration()
                    {
                        Port = Port2,
                        ServerName = ServerName2
                    })
                    {
                        RunInMemory = true,
                        UseEmbeddedHttpServer = true
                    }.Initialize())
                    {
                        var doc2 = MultiDatabase.CreateDatabaseDocument("db2");
                        ((ServerClient)Server.DocumentStore.DatabaseCommands.ForSystemDatabase()).GlobalAdmin.CreateDatabase(doc2);

                        using (var store2 = new DocumentStore
                        {
                            HttpMessageHandler = Server.DocumentStore.HttpMessageHandler,
                            Url = "http://localhost:8082",
                            DefaultDatabase = "db2"
                        }.Initialize())
                        {

                            var smugglerApi = new SmugglerApi();
                            smugglerApi.Between(new SmugglerBetweenOptions
                            {
                                From = new RavenConnectionStringOptions { Url = "http://localhost:8081", DefaultDatabase = "db1" },
                                To = new RavenConnectionStringOptions { Url = "http://localhost:8082", DefaultDatabase = "db2" }
                            });

                            var indexes = store2.DatabaseCommands.GetIndexes(0,10);
                            Assert.Equal(1, indexes.Length);
                            var transformers = store2.DatabaseCommands.GetTransformers(0, 10);
                            Assert.Equal(1, transformers.Length);
                            var attachments = store2.DatabaseCommands.GetAttachments(0, null, 10);
                            Assert.Equal(1, attachments.Length);

                        }
                    }
                }
            }
        }
    }
}
