using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16301 : RavenTestBase
    {
        public RavenDB_16301(ITestOutputHelper output) : base(output)
        {
        }

        private class Result
        {
            public string Id { get; set; }
            public string ChangeVector { get; set; }
        }

        [Fact]
        public void CanUseConditionalLoadLazily()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        bulkInsert.Store(new Company());
                    }
                }

                var ids = new List<Result>();
                var loads = new List<Lazy<(Company Entity, string ChangeVector)>>();

                using (var session1 = store.OpenSession())
                {
                    ids = session1.Advanced.DocumentQuery<Company>()
                           .WaitForNonStaleResults()
                           .SelectFields<Result>(QueryData.CustomFunction(
                               alias: "o",
                               func: "{ Id : id(o), ChangeVector : getMetadata(o)['@change-vector'] }")
                           ).ToList();

                    session1.Load<Company>(ids.Select(x => x.Id).ToList());

                    var res = session1.Load<Company>(ids.Take(50).Select(x => x.Id).ToList());
                    var c = 0;
                    foreach (var kvp in res)
                    {
                        kvp.Value.Phone = ++c;
                    }
                    session1.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // load last 10
                    session.Load<Company>(ids.Skip(90).Take(10).Select(x => x.Id).ToList());
                    var numberOfRequestsPerSession = session.Advanced.NumberOfRequests;
                    foreach (var res in ids)
                    {
                        loads.Add(session.Advanced.Lazily.ConditionalLoad<Company>(res.Id, res.ChangeVector));
                    }

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    Assert.Equal(numberOfRequestsPerSession + 1, session.Advanced.NumberOfRequests);
                    for (int i = 0; i < 100; i++)
                    {
                        var l = loads[i];

                        Assert.False(l.IsValueCreated);
                        (Company entity, string changeVector) = l.Value;

                        if (i < 50)
                        {
                            // load from server
                            Assert.Equal(ids[i].Id, entity.Id);
                            Assert.NotEqual(ids[i].ChangeVector, changeVector);
                        }
                        else if (i < 90)
                        {
                            // mot modified
                            Assert.Null(entity);
                            Assert.Equal(ids[i].ChangeVector, changeVector);
                        }
                        else
                        {
                            // tracked in the session
                            Assert.Equal(ids[i].Id, entity.Id);
                            Assert.NotNull(entity);
                            Assert.Equal(ids[i].ChangeVector, changeVector);
                        }

                        // not exist on server
                        var lazy = session.Advanced.Lazily.ConditionalLoad<Company>("Companies/322-A", ids[0].ChangeVector);
                        var load = lazy.Value;
                        Assert.Null(load.Entity);
                        Assert.Null(load.ChangeVector);
                    }
                }
            }
        }

        [Fact]
        public async Task CanUseConditionalAsyncLoadLazily()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await bulkInsert.StoreAsync(new Company());
                    }
                }

                var ids = new List<Result>();
                var loads = new List<Lazy<Task<(Company Entity, string ChangeVector)>>>();

                using (var session1 = store.OpenAsyncSession())
                {
                    ids = await session1.Advanced.AsyncDocumentQuery<Company>()
                           .WaitForNonStaleResults()
                           .SelectFields<Result>(QueryData.CustomFunction(
                               alias: "o",
                               func: "{ Id : id(o), ChangeVector : getMetadata(o)['@change-vector'] }")
                           ).ToListAsync();

                    await session1.LoadAsync<Company>(ids.Select(x => x.Id).ToList());

                    var res = await session1.LoadAsync<Company>(ids.Take(50).Select(x => x.Id).ToList());
                    var c = 0;
                    foreach (var kvp in res)
                    {
                        kvp.Value.Phone = ++c;
                    }
                    await session1.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    // load last 10
                    await session.LoadAsync<Company>(ids.Skip(90).Take(10).Select(x => x.Id).ToList());

                    var numberOfRequestsPerSession = session.Advanced.NumberOfRequests;
                    foreach (var res in ids)
                    {
                        loads.Add(session.Advanced.Lazily.ConditionalLoadAsync<Company>(res.Id, res.ChangeVector));
                    }

                    await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

                    Assert.Equal(numberOfRequestsPerSession + 1, session.Advanced.NumberOfRequests);
                    for (int i = 0; i < 100; i++)
                    {
                        var l = loads[i];

                        Assert.False(l.IsValueCreated);
                        (Company entity, string changeVector) = await l.Value;

                        if (i < 50)
                        {
                            // load from server
                            Assert.Equal(ids[i].Id, entity.Id);
                            Assert.NotNull(entity);
                            Assert.NotEqual(ids[i].ChangeVector, changeVector);
                        }
                        else if (i < 90)
                        {
                            // mot modified
                            Assert.Null(entity);
                            Assert.Equal(ids[i].ChangeVector, changeVector);
                        }
                        else
                        {
                            // tracked in the session
                            Assert.Equal(ids[i].Id, entity.Id);
                            Assert.NotNull(entity);
                            Assert.Equal(ids[i].ChangeVector, changeVector);
                        }

                        // not exist on server
                        var lazy = session.Advanced.Lazily.ConditionalLoadAsync<Company>("Companies/322-A", ids[0].ChangeVector);
                        var load = await lazy.Value;
                        Assert.Null(load.Entity);
                        Assert.Null(load.ChangeVector);
                    }
                }
            }
        }
    }
}
