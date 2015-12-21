#if !DNXCORE50
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Raven.Tests.Core.Utils.Transformers;
using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Core.Smuggler
{
    public class SmugglerApiTests : RavenCoreTestBase
    {
        public const int Port1 = 8077;
        public const int Port2 = 8078;
        public const string ServerName1 = "Raven.Tests.Core.Server";
        public const string ServerName2 = "Raven.Tests.Core.Server2";

        private string BackupDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Backup");

        public SmugglerApiTests()
        {
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(Port1);
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(Port2);
            IOExtensions.DeleteDirectory(BackupDir);
        }

        public override void Dispose()
        {
            base.Dispose();
            IOExtensions.DeleteDirectory(BackupDir);
        }

        [Fact]
        public async Task CanUseBetween()
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
                ((ServerClient)server1.DocumentStore.DatabaseCommands.ForSystemDatabase()).GlobalAdmin.CreateDatabase(doc);

                using (var store1 = new DocumentStore
                {
                    Url = "http://localhost:" + Port1,
                    DefaultDatabase = "db1"
                }.Initialize())
                {
                    new Users_ByName().Execute(store1);
                    new UsersTransformer().Execute(store1);

                    using (var session = store1.OpenSession("db1"))
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
                        ((ServerClient)server2.DocumentStore.DatabaseCommands.ForSystemDatabase()).GlobalAdmin.CreateDatabase(doc2);

                        using (var store2 = new DocumentStore
                        {
                            Url = "http://localhost:" + Port2,
                            DefaultDatabase = "db2"
                        }.Initialize())
                        {

                            var smugglerApi = new SmugglerDatabaseApi();
                            await smugglerApi.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
                            {
                                From = new RavenConnectionStringOptions { Url = "http://localhost:" + Port1, DefaultDatabase = "db1" },
                                To = new RavenConnectionStringOptions { Url = "http://localhost:" + Port2, DefaultDatabase = "db2" }
                            });

                            var docs = store2.DatabaseCommands.GetDocuments(0, 10);
                            Assert.Equal(3, docs.Length);
                            var indexes = store2.DatabaseCommands.GetIndexes(0,10);
                            Assert.Equal(1, indexes.Length);
                            var transformers = store2.DatabaseCommands.GetTransformers(0, 10);
                            Assert.Equal(1, transformers.Length);
                            var attachments = store2.DatabaseCommands.GetAttachments(0, new Etag(), 10);
                            Assert.Equal(1, attachments.Length);

                        }
                    }
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanExportAndImportData(bool disableCompressionOnImport)
        {
            using (var server1 = new RavenDbServer(new RavenConfiguration
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
                ((ServerClient)server1.DocumentStore.DatabaseCommands.ForSystemDatabase()).GlobalAdmin.CreateDatabase(doc);

                using (var store1 = new DocumentStore
                {
                    Url = "http://localhost:" + Port1,
                    DefaultDatabase = "db1"
                }.Initialize())
                {
                    new Users_ByName().Execute(store1);
                    new UsersTransformer().Execute(store1);

                    using (var session = store1.OpenSession("db1"))
                    {
                        session.Store(new User { Name = "Name1", LastName = "LastName1" });
                        session.Store(new User { Name = "Name2", LastName = "LastName2" });
                        session.SaveChanges();
                    }

                    store1.DatabaseCommands.PutAttachment("attachement1", null, new MemoryStream(new byte[] { 3 }), new RavenJObject());

                    var smugglerApi = new SmugglerDatabaseApi
                    (
                        new SmugglerDatabaseOptions
                        {
                            DisableCompressionOnImport = disableCompressionOnImport
                        }
                    );

                    await smugglerApi.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions> 
                        { 
                            From = new RavenConnectionStringOptions { Url = "http://localhost:" + Port1, DefaultDatabase = "db1" },
                            ToFile = BackupDir,							
                        });

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
                        ((ServerClient)server2.DocumentStore.DatabaseCommands.ForSystemDatabase()).GlobalAdmin.CreateDatabase(doc2);

                        using (var store2 = new DocumentStore
                        {
                            Url = "http://localhost:" + Port2,
                            DefaultDatabase = "db2"
                        }.Initialize())
                        {
                            await smugglerApi.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                            {
                                FromFile = BackupDir,
                                To = new RavenConnectionStringOptions { Url = "http://localhost:" + Port2, DefaultDatabase = "db2" }
                            });
                            
                            var docs = store2.DatabaseCommands.GetDocuments(0, 10);
                            Assert.Equal(3, docs.Length);
                            var indexes = store2.DatabaseCommands.GetIndexes(0,10);
                            Assert.Equal(1, indexes.Length);
                            var transformers = store2.DatabaseCommands.GetTransformers(0, 10);
                            Assert.Equal(1, transformers.Length);
                            var attachments = store2.DatabaseCommands.GetAttachments(0, new Etag(), 10);
                            Assert.Equal(1, attachments.Length);
                        }
                    }
                }
            }
        }
    }
}
#endif