using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_10815
    {
        public class NagleSession : RavenTestBase
        {
            [Fact]
            public void EmbeddedNagleSession()
            {
                using (var store = NewDocumentStore())
                {
                    Parallel.For(0, 10, _ =>
                    {
                        using (var session = store.OpenNagleSession())
                        {
                            var entity1 = new Entity();
                            session.Store(entity1);
                            var entity2 = new Entity();
                            session.Store(entity2);
                            session.SaveChanges();
                        }
                    });

                    var statistics = store.DatabaseCommands.GetStatistics();
                    Assert.Equal(21, statistics.CountOfDocuments); // including the hilo document
                }
            }

            [Fact]
            public void EmbeddedNagleAsyncSession()
            {
                using (var store = NewDocumentStore())
                {
                    Parallel.For(0, 10, (_) =>
                    {
                        AsyncHelpers.RunSync(() => AddEntitiesAsync(store));
                    });

                    var statistics = store.DatabaseCommands.GetStatistics();
                    Assert.Equal(21, statistics.CountOfDocuments); // including the hilo document
                }
            }

            [Fact]
            public void RemoteNagleSession()
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Parallel.For(0, 10, _ =>
                    {
                        using (var session = store.OpenNagleSession())
                        {
                            var entity1 = new Entity();
                            session.Store(entity1);
                            var entity2 = new Entity();
                            session.Store(entity2);
                            session.SaveChanges();
                        }
                    });

                    var statistics = store.DatabaseCommands.GetStatistics();
                    Assert.Equal(21, statistics.CountOfDocuments); // including the hilo document
                }
            }

            [Fact]
            public void RemoteNagleAsyncSession()
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Parallel.For(0, 10, _ =>
                    {
                        AsyncHelpers.RunSync(() => AddEntitiesAsync(store));
                    });

                    var statistics = store.DatabaseCommands.GetStatistics();
                    Assert.Equal(21, statistics.CountOfDocuments); // including the hilo document
                }
            }

            [Fact]
            public void NagleSessionWithFailure()
            {
                using (var store = NewDocumentStore())
                {
                    const string id = "test";
                    using (var session = store.OpenNagleSession())
                    {
                        var entity = new Entity {Id = id };
                        session.Store(entity);
                        session.SaveChanges();
                    }

                    var concurrencyCount = 0;
                    Parallel.For(0, 10, i =>
                    {
                        try
                        {
                            using (var session = store.OpenNagleSession())
                            {
                                session.Advanced.UseOptimisticConcurrency = true;
                                var entity = session.Load<Entity>(id);
                                entity.Tokens = new List<string>
                                {
                                    i.ToString()
                                };
                                var newEntity = new Entity();
                                session.Store(newEntity);
                                session.SaveChanges();
                            }
                        }
                        catch (ConcurrencyException)
                        {
                            concurrencyCount++;
                        }
                    });

                    Assert.True(concurrencyCount <= 9 && concurrencyCount > 5);
                    var statistics = store.DatabaseCommands.GetStatistics();
                    // 1 edited document, 1 hilo document, at least 1 new document
                    Assert.True(statistics.CountOfDocuments >= 3);
                }
            }

            [Fact]
            public void NagleAsyncSessionWithFailure()
            {
                using (var store = NewDocumentStore())
                {
                    const string id = "test";
                    using (var session = store.OpenNagleSession())
                    {
                        var entity = new Entity { Id = id };
                        session.Store(entity);
                        session.SaveChanges();
                    }

                    var concurrencyCount = 0;
                    Parallel.For(0, 10, i =>
                    {
                        try
                        {
                            AsyncHelpers.RunSync(() => EditEntityAsync(store, id, i));
                        }
                        catch (ConcurrencyException)
                        {
                            concurrencyCount++;
                        }
                    });

                    Assert.True(concurrencyCount <= 9 && concurrencyCount > 5);
                    var statistics = store.DatabaseCommands.GetStatistics();
                    // at least 1 edited document, 1 hilo document, at least 1 new document
                    Assert.True(statistics.CountOfDocuments >= 3);
                }
            }

            [Fact]
            public void TwoDatabasesNagleSession()
            {
                const string db1 = "db1";
                const string db2 = "db2";
                using (var store = NewRemoteDocumentStore())
                {
                    store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                    {
                        Id = db1,
                        Settings =
                        {
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });
                    store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
                    {
                        Id = db2,
                        Settings =
                        {
                            { "Raven/DataDir", NewDataPath() }
                        }
                    });

                    Parallel.For(0, 20, i =>
                    {
                        using (var session = i % 2 == 0 ? store.OpenNagleSession(db1) : store.OpenNagleSession(db2))
                        {
                            var entity1 = new Entity();
                            session.Store(entity1);
                            var entity2 = new Entity();
                            session.Store(entity2);
                            session.SaveChanges();
                        }
                    });

                    var statistics = store.DatabaseCommands.ForDatabase(db1).GetStatistics();
                    Assert.Equal(21, statistics.CountOfDocuments); // including the hilo document

                    statistics = store.DatabaseCommands.ForDatabase(db2).GetStatistics();
                    Assert.Equal(21, statistics.CountOfDocuments); // including the hilo document
                }
            }

            private static async Task AddEntitiesAsync(IDocumentStore store)
            {
                using (var session = store.OpenNagleAsyncSession())
                {
                    var entity1 = new Entity();
                    await session.StoreAsync(entity1);
                    var entity2 = new Entity();
                    await session.StoreAsync(entity2);
                    await session.SaveChangesAsync();
                }
            }

            private static async Task EditEntityAsync(EmbeddableDocumentStore store, string id, int i)
            {
                using (var session = store.OpenNagleAsyncSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    var entity = await session.LoadAsync<Entity>(id);
                    entity.Tokens = new List<string>
                    {
                        i.ToString()
                    };
                    var newEntity = new Entity();
                    await session.StoreAsync(newEntity);
                    await session.SaveChangesAsync();
                }
            }

            internal class Entity
            {
                public string Id { get; set; }
                public List<string> Tokens { get; set; }
            }
        }
    }
}
