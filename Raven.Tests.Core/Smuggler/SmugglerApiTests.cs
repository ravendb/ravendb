using Raven.Abstractions.Data;
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

using Raven.Abstractions.Database.Smuggler;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Files;
using Raven.Smuggler.Database.Remote;
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
                Core =
                {
                    Port = Port1
                },
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

                    using (var server2 = new RavenDbServer(new RavenConfiguration()
                    {
                        Core =
                        {
                            Port = Port2
                        },
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
                            var smuggler = new DatabaseSmuggler(
                                new DatabaseSmugglerOptions(),
                                new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                            {
                                    Url = "http://localhost:" + Port1,
                                    Database = "db1"
                                }),
                                new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                                {
                                    Url = "http://localhost:" + Port2,
                                    Database = "db2"
                                }));

                            await smuggler.ExecuteAsync();

                            var docs = store2.DatabaseCommands.GetDocuments(0, 10);
                            Assert.Equal(3, docs.Length);
                            var indexes = store2.DatabaseCommands.GetIndexes(0, 10);
                            Assert.Equal(1, indexes.Length);
                            var transformers = store2.DatabaseCommands.GetTransformers(0, 10);
                            Assert.Equal(1, transformers.Length);
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
                Core =
                {
                    Port = Port1
                },
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

                    var connectionOptions = new DatabaseSmugglerRemoteConnectionOptions
                        {
                        Url = "http://localhost:" + Port1,
                        Database = "db1"
                    };

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerRemoteSource(connectionOptions),
                        new DatabaseSmugglerFileDestination(BackupDir));

                    await smuggler.ExecuteAsync();

                    using (var server2 = new RavenDbServer(new RavenConfiguration()
                    {
                        Core =
                        {
                        Port = Port2,
                        },
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
                            connectionOptions = new DatabaseSmugglerRemoteConnectionOptions
                            {
                                Url = "http://localhost:" + Port2,
                                Database = "db2"
                            };
                            
                            smuggler = new DatabaseSmuggler(
                                new DatabaseSmugglerOptions(),
                                new DatabaseSmugglerFileSource(BackupDir),
                                new DatabaseSmugglerRemoteDestination(
                                    connectionOptions,
                                    new DatabaseSmugglerRemoteDestinationOptions
                                    {
                                        DisableCompression = disableCompressionOnImport
                                    }));

                            await smuggler.ExecuteAsync();

                            var docs = store2.DatabaseCommands.GetDocuments(0, 10);
                            Assert.Equal(3, docs.Length);
                            var indexes = store2.DatabaseCommands.GetIndexes(0, 10);
                            Assert.Equal(1, indexes.Length);
                            var transformers = store2.DatabaseCommands.GetTransformers(0, 10);
                            Assert.Equal(1, transformers.Length);
                        }
                    }
                }
            }
        }
    }
}
