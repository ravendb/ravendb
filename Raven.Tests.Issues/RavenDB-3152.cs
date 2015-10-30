using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Common;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Database.Extensions;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Files;
using Raven.Smuggler.Database.Remote;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3152 : RavenTestBase
    {
        [Fact]
        public async Task Smuggler_filtering_next_etag()
        {
            using (var server = GetNewServer())
            {
                using (var store = new DocumentStore {Url = server.SystemDatabase.Configuration.ServerUrl}.Initialize())
                {
                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Dba1",
                            Settings =
                            {
                                {"Raven/DataDir", "Dba1"}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Dba1");

                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Dba2",
                            Settings =
                            {
                                {"Raven/DataDir", "Dba2"}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Dba2");
                }
                using (var store1 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba1"
                }.Initialize())
                {
                    StoreWorkerseDba1(store1);
                    using (var session = store1.OpenSession())
                    {
                        var workers = session.Query<Worker>().ToList();
                        Assert.Equal(3, workers.Count);

                        var index1 = store1.DatabaseCommands.GetIndex("WorkerByName");
                        var index2 = store1.DatabaseCommands.GetIndex("WorkerByAge");
                        var index3 = store1.DatabaseCommands.GetIndex("WorkerAccountNumber");
                        Assert.Equal("WorkerByName", index1.Name);
                        Assert.Equal("WorkerByAge", index2.Name);
                        Assert.Equal("WorkerAccountNumber", index3.Name);
                    }
                }

                var options = new DatabaseSmugglerOptions();
                options.Filters.Add(new FilterSetting
                {
                    Path = "Name",
                    ShouldMatch = true,
                    Values = { "worker/22", "worker/333" }
                });

                var smuggler = new DatabaseSmuggler(
                    options,
                    new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = server.SystemDatabase.Configuration.ServerUrl,
                        Database = "Dba1"
                    }),
                    new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = server.SystemDatabase.Configuration.ServerUrl,
                        Database = "Dba2"
                    }));

                await smuggler.ExecuteAsync();

                using (var store2 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba2"

                }.Initialize())
                {
                    using (var session = store2.OpenSession())
                    {
                        var workers = session.Query<Worker>().ToList();
                        Assert.Equal(0, workers.Count);
                        var index1 = store2.DatabaseCommands.GetIndex("WorkerByName");
                        var index2 = store2.DatabaseCommands.GetIndex("WorkerByAge");
                        var index3 = store2.DatabaseCommands.GetIndex("WorkerAccountNumber");
                        Assert.Equal("WorkerByName", index1.Name);
                        Assert.Equal("WorkerByAge", index2.Name);
                        Assert.Equal("WorkerAccountNumber", index3.Name);
                    }
                }
            }
        }

        [Fact]
        public async Task Smuggler_filtering_next_etag_incremental_between()
        {
            using (var server = GetNewServer())
            {
                using (var store = new DocumentStore { Url = server.SystemDatabase.Configuration.ServerUrl }.Initialize())
                {
                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Dba1",
                            Settings =
                            {
                                {"Raven/DataDir", "Dba1"}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Dba1");

                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Dba2",
                            Settings =
                            {
                                {"Raven/DataDir", "Dba2"}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Dba2");
                }
                using (var store1 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba1"
                }.Initialize())
                {
                    StoreWorkerseDba1(store1);
                    using (var session = store1.OpenSession())
                    {
                        var workers = session.Query<Worker>().ToList();
                        Assert.Equal(3, workers.Count);

                        var index1 = store1.DatabaseCommands.GetIndex("WorkerByName");
                        var index2 = store1.DatabaseCommands.GetIndex("WorkerByAge");
                        var index3 = store1.DatabaseCommands.GetIndex("WorkerAccountNumber");
                        Assert.Equal("WorkerByName", index1.Name);
                        Assert.Equal("WorkerByAge", index2.Name);
                        Assert.Equal("WorkerAccountNumber", index3.Name);
                    }
                }

                var options = new DatabaseSmugglerOptions
                {
                    OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes
                };
                options.Filters.Add(new FilterSetting
                {
                    Path = "Name",
                    ShouldMatch = true,
                    Values = { "worker/22", "worker/333" }
                });

                var smuggler = new DatabaseSmuggler(
                    options,
                    new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = server.SystemDatabase.Configuration.ServerUrl,
                        Database = "Dba1"
                    }),
                    new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = server.SystemDatabase.Configuration.ServerUrl,
                        Database = "Dba2"
                    }));

                await smuggler.ExecuteAsync();

                using (var store2 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba2"

                }.Initialize())
                {
                    using (var session = store2.OpenSession())
                    {
                        var workers = session.Query<Worker>().ToList();
                        Assert.Equal(0, workers.Count);
                        var index1 = store2.DatabaseCommands.GetIndex("WorkerByName");
                        var index2 = store2.DatabaseCommands.GetIndex("WorkerByAge");
                        var index3 = store2.DatabaseCommands.GetIndex("WorkerAccountNumber");
                        Assert.Equal("WorkerByName", index1.Name);
                        Assert.Equal("WorkerByAge", index2.Name);
                        Assert.Equal("WorkerAccountNumber", index3.Name);
                    }
                }

                using (var store1 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba1"
                }.Initialize())
                {
                    StoreWorkerseDba1(store1);
                }

                options = new DatabaseSmugglerOptions
                {
                    OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes
                };
                options.Filters.Add(new FilterSetting
                {
                    Path = "Name",
                    ShouldMatch = true,
                    Values = { "worker/22", "worker/33" }
                });

                smuggler = new DatabaseSmuggler(
                    options,
                    new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = server.SystemDatabase.Configuration.ServerUrl,
                        Database = "Dba1"
                    }),
                    new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = server.SystemDatabase.Configuration.ServerUrl,
                        Database = "Dba2"
                    }));

                await smuggler.ExecuteAsync();

                using (var store2 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba2"

                }.Initialize())
                {
                    using (var session = store2.OpenSession())
                    {
                        var workers = session.Query<Worker>().ToList();
                        Assert.Equal(0, workers.Count);
                        var index1 = store2.DatabaseCommands.GetIndex("WorkerByName");
                        var index2 = store2.DatabaseCommands.GetIndex("WorkerByAge");
                        var index3 = store2.DatabaseCommands.GetIndex("WorkerAccountNumber");
                        Assert.Equal("WorkerByName", index1.Name);
                        Assert.Equal("WorkerByAge", index2.Name);
                        Assert.Equal("WorkerAccountNumber", index3.Name);
                    }
                }

            }
        }
        [Fact]
        public async Task Smuggler_filtering_next_etag_incremental_export_to_file()
        {
            using (var server = GetNewServer())
            {
                var file = Path.Combine(NewDataPath(), "Incremental");
                IOExtensions.DeleteDirectory(file);
                using (var store = new DocumentStore { Url = server.SystemDatabase.Configuration.ServerUrl }.Initialize())
                {
                    store
                        .DatabaseCommands
                        .GlobalAdmin
                        .CreateDatabase(new DatabaseDocument
                        {
                            Id = "Dba1",
                            Settings =
                            {
                                {"Raven/DataDir", "Dba1"}
                            }
                        });
                    store.DatabaseCommands.EnsureDatabaseExists("Dba1");
                }
                using (var store1 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba1"
                }.Initialize())
                {
                    StoreWorkerseDba1(store1);
                    using (var session = store1.OpenSession())
                    {
                        var workers = session.Query<Worker>().ToList();
                        Assert.Equal(3, workers.Count);

                        var index1 = store1.DatabaseCommands.GetIndex("WorkerByName");
                        var index2 = store1.DatabaseCommands.GetIndex("WorkerByAge");
                        var index3 = store1.DatabaseCommands.GetIndex("WorkerAccountNumber");
                        Assert.Equal("WorkerByName", index1.Name);
                        Assert.Equal("WorkerByAge", index2.Name);
                        Assert.Equal("WorkerAccountNumber", index3.Name);
                    }
                }

                var options = new DatabaseSmugglerOptions
                {
                    OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes
                };
                options.Filters.Add(new FilterSetting
                {
                    Path = "Name",
                    ShouldMatch = true,
                    Values = { "worker/21", "worker/33" }
                });

                var smuggler = new DatabaseSmuggler(
                    options,
                    new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = server.SystemDatabase.Configuration.ServerUrl,
                        Database = "Dba1"
                    }),
                    new DatabaseSmugglerFileDestination(
                        file, 
                        new DatabaseSmugglerFileDestinationOptions
                        {
                            Incremental = true
                        }));

                await smuggler.ExecuteAsync();

                //check file after first export

                using (var store1 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba1"
                }.Initialize())
                {

                    StoreWorkerseDba1(store1);

                }


                //Second time Export 

                await smuggler.ExecuteAsync();

                using (var store2 = new DocumentStore
                {
                    Url = server.SystemDatabase.Configuration.ServerUrl,
                    DefaultDatabase = "Dba2"

                }.Initialize())
                {

                    options = new DatabaseSmugglerOptions
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes
                    };

                    smuggler = new DatabaseSmuggler(
                        options,
                        new DatabaseSmugglerFileSource(file),
                        new DatabaseSmugglerRemoteDestination(
                            new DatabaseSmugglerRemoteConnectionOptions
                            {
                                Url = server.SystemDatabase.Configuration.ServerUrl,
                                Database = "Dba2"
                            }));

                    await smuggler.ExecuteAsync();

                    using (var session = store2.OpenSession())
                    {
                        var workers = session.Query<Worker>().ToList();
                        Assert.Equal(0, workers.Count);
                    }
                }
            }
        }

        public void StoreWorkerseDba1(IDocumentStore docStore)
        {
            using (var session = docStore.OpenSession())
            {
                session.Store(new Worker { Name = "worker/1", Age = 20, AccountNumber = 1536 });
                session.Store(new Worker { Name = "worker/2", Age = 40, AccountNumber = 2006 });
                session.Store(new Worker { Name = "worker/3", Age = 30, AccountNumber = 1106 });
                session.SaveChanges();
            }
            new WorkerByName().Execute(docStore);
            new WorkerByAge().Execute(docStore);
            new WorkerAccountNumber().Execute(docStore);

            WaitForIndexing(docStore);
        }
    }

    public class Worker
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public int AccountNumber { get; set; }
    }

    public class WorkerByName : AbstractIndexCreationTask<Worker>
    {
        public WorkerByName()
        {
            Map = workers => from worker in workers
                             select new { worker.Name };
            Index(x => x.Name, FieldIndexing.Analyzed);

        }
    }

    public class WorkerByAge : AbstractIndexCreationTask<Worker>
    {
        public WorkerByAge()
        {
            Map = workers => from worker in workers
                select new {worker.Age};
            Sort(x => x.Age, SortOptions.Int);

        }
    }

    public class WorkerAccountNumber : AbstractIndexCreationTask<Worker>
    {
        public WorkerAccountNumber()
        {
            Map = workers => from worker in workers
                             select new { worker.AccountNumber };

        }
    }
}
