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
using Xunit.Sdk;

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
                Category c = new Category { Name = $"n0", Description = $"d0" };
                using (var session = store.OpenAsyncSession())
                {
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
                Exception exception = null;
                AssertActualExpectedException assertException = null;
                database.ForTestingPurposesOnly().CompactionAfterDatabaseUnload = () =>
                {
                    try
                    {
                        using (var session = store.OpenSession())
                        {
                            var l = session.Query<Categoroies_Details.Entity, Categoroies_Details>()
                                                    .Where(x => x.Id == categoryId)
                                                    .ProjectInto<Categoroies_Details.Entity>()
                                                    .ToList();

                            Assert.NotNull(l);
                            Assert.Equal(1, l.Count);
                            Assert.Equal(categoryId, l[0].Id);
                            Assert.Equal($"Id=\"{c.Id}\", Name=\"{c.Name}\", Description=\"{c.Description}\"", l[0].Details);
                        }
                    }
                    catch (AssertActualExpectedException e)
                    {
                        assertException = e;
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }
                };

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(settings));
                await operation.WaitForCompletionAsync();

                if (cluster == false)
                {
                    Assert.NotNull(exception);
                    Assert.True(exception is DatabaseDisabledException);
                }
                else
                {
                    Assert.Null(exception); // Failover

                    if (assertException != null) // callback inner asserts
                        throw assertException;
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
                Category c = new Category { Name = $"n0", Description = $"d0" };
                using (var session = store.OpenAsyncSession())
                {
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
                Exception exception = null;
                AssertActualExpectedException assertException = null;
                database.IndexStore.ForTestingPurposesOnly().IndexCompaction = () =>
                {
                    try
                    {
                        using (var session = store.OpenSession())
                        {
                            var l = session.Query<Categoroies_Details.Entity, Categoroies_Details>()
                                                    .Where(x => x.Id == categoryId)
                                                    .ProjectInto<Categoroies_Details.Entity>()
                                                    .ToList();

                            Assert.NotNull(l);
                            Assert.Equal(1, l.Count);
                            Assert.Equal(categoryId, l[0].Id);
                            Assert.Equal($"Id=\"{c.Id}\", Name=\"{c.Name}\", Description=\"{c.Description}\"", l[0].Details);
                        }
                    }
                    catch (AssertActualExpectedException e)
                    {
                        assertException = e;
                    }
                    catch (Exception e)
                    {
                        exception = e;
                    }
                };

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(settings));
                await operation.WaitForCompletionAsync();

                if (cluster == false)
                {
                    Assert.NotNull(exception);
                    Assert.True(exception is IndexCompactionInProgressException);
                }
                else
                {
                    Assert.Null(exception); // Failover

                    if (assertException != null) // callback inner asserts
                        throw assertException;
                }
            }
        }

        class Categoroies_Details : AbstractMultiMapIndexCreationTask<Categoroies_Details.Entity>
        {
            internal class Entity
            {
                public string Id { get; set; }
                public string Details { get; set; }
            }

            public Categoroies_Details()
            {
                AddMap<Category>(
                    categories =>
                        from c in categories
                        select new Entity
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

