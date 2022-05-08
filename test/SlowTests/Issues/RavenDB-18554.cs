using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using NuGet.ContentModel;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18554 : ClusterTestBase
    {
        public RavenDB_18554(ITestOutputHelper output) : base(output)
        {
        }


        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task QueriesShouldFailoverIfDatabaseIsCompacting(bool cluster = false)
        {
            Options storeOptions;
            RavenServer leader = null;
            if (cluster)
            {
                List<RavenServer> nodes;
                (nodes, leader) = await CreateRaftCluster(2);
                storeOptions = new Options {Server = leader, ReplicationFactor = nodes.Count, RunInMemory = false};
            }
            else
            {
                storeOptions = new Options {RunInMemory = false};
            }
            
            using (var store = GetDocumentStore(storeOptions))
            {
                // Prepare Server For Test
                string categoryId;
                using (var session = store.OpenAsyncSession())
                {
                    Category c = new Category {Name = $"n0", Description = $"d0"};
                    await session.StoreAsync(c);
                    await session.SaveChangesAsync();
                    categoryId = c.Id;
                }
                
                var index = new Categoroies_Details();
                index.Execute(store);
                Indexes.WaitForIndexing(store);

                string[] indexNames = { index.IndexName };
                CompactSettings settings = new CompactSettings {DatabaseName = store.Database, Documents = true, Indexes = indexNames};

                var database = cluster ? await GetDatabase(leader, store.Database) : await GetDatabase(store.Database);

                // Test
                Exception e = null;
                database.ForTestingPurposesOnly().CompactionAfterDatabaseUnloadAction = () =>
                {
                    try
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Query<Categoroies_Details.Entitiy, Categoroies_Details>()
                                .Where(x => x.Id == categoryId)
                                .ProjectInto<Categoroies_Details.Entitiy>()
                                .ToList();
                        }
                    }
                    catch (Exception e1)
                    {
                        e = e1;
                    }
                };

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(settings));
                await operation.WaitForCompletionAsync();

                if (!cluster)
                {
                    Assert.NotNull(e);
                    Assert.True(e is DatabaseDisabledException);
                }
                else
                {
                    Assert.Null(e); // Faliover
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task QueriesShouldFailoverIfIndexIsCompacting(bool cluster = false)
        {
            Options storeOptions;
            RavenServer leader = null;
            if (cluster)
            {
                List<RavenServer> nodes;
                (nodes, leader) = await CreateRaftCluster(2);
                storeOptions = new Options { Server = leader, ReplicationFactor = nodes.Count, RunInMemory = false };
            }
            else
            {
                storeOptions = new Options { RunInMemory = false };
            }

            using (var store = GetDocumentStore(storeOptions))
            {
                // Prepare Server For Test
                string categoryId;
                using (var session = store.OpenAsyncSession())
                {
                    Category c = new Category { Name = $"n0", Description = $"d0" };
                    await session.StoreAsync(c);
                    await session.SaveChangesAsync();
                    categoryId = c.Id;
                }

                var index = new Categoroies_Details();
                index.Execute(store);
                Indexes.WaitForIndexing(store);

                string[] indexNames = { index.IndexName };
                CompactSettings settings = new CompactSettings { DatabaseName = store.Database, Documents = true, Indexes = indexNames };
                var database = cluster ? await GetDatabase(leader, store.Database) : await GetDatabase(store.Database);

                // Test
                Exception e = null;
                database.ForTestingPurposesOnly().IndexCompaction = () =>
                {
                    try
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Query<Categoroies_Details.Entitiy, Categoroies_Details>()
                                .Where(x => x.Id == categoryId)
                                .ProjectInto<Categoroies_Details.Entitiy>()
                                .ToList();
                        }
                    }
                    catch (Exception e1)
                    {
                        e = e1;
                    }
                };

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(settings));
                await operation.WaitForCompletionAsync();


                if (!cluster)
                {
                    Assert.NotNull(e);
                    Assert.True(e is IndexCompactionInProgressException);
                }
                else
                {
                    Assert.Null(e); // Faliover
                }
            }
        }

        class Categoroies_Details : AbstractMultiMapIndexCreationTask<Categoroies_Details.Entitiy>
        {
            internal class Entitiy
            {
                public string Id { get; set; }
                public string Details { get; set; }

                public override string ToString()
                {
                    return $"Id=\"{Id}\", Details=\"{Details}\"";
                }
            }

            public Categoroies_Details()
            {
                AddMap<Category>(
                    categories =>
                        from c in categories
                        select new Entitiy
                        {
                            Id = c.Id,
                            Details = $"Id=\"{c.Id}\", Name=\"{c.Name}\", Description=\"{c.Description}\""
                        }
                );
                Store(x => x.Details, FieldStorage.Yes);
            }
        }
    }
}

