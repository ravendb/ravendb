using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class WaitingForNonStaleResults : RavenTestBase
    {
        public WaitingForNonStaleResults(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Cutoff_etag_usage()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    var entity = new User();
                    await session.StoreAsync(entity);

                    await session.StoreAsync(new Address());
                    await session.StoreAsync(new Address());

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal(2, users.Count);
                }
            }
        }

        [Fact]
        public async Task As_of_now_usage()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.StoreAsync(new User());

                    await session.StoreAsync(new Address());
                    await session.StoreAsync(new Address());

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Name).ToList();

                    Assert.Equal(2, users.Count);
                }
            }
        }

        [Fact]
        public void Throws_if_exceeds_timeout()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new Address());
                    session.Store(new Address());

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var sp = Stopwatch.StartNew();
                    Assert.Throws<TimeoutException>(() =>
                        session.Query<Address>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMilliseconds(1)))
                        .OrderBy(x => x.City)
                        .ToList()
                    );

                    var timeout = 1000;
                    if (Debugger.IsAttached)
                        timeout *= 25;
                    Assert.True(sp.ElapsedMilliseconds < timeout, sp.Elapsed.ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task CanWaitForNonStaleResultsForAllDocsIndex()
        {
            const string companyName1 = "Hibernating Rhinos";
            const string companyName2 = "RavenDB";

            using (var store = GetDocumentStore())
            {
                var index = new AllDocsMapIndex();
                await index.ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    await session.StoreAsync(new Company { Name = companyName1 });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<AllDocsMapIndex.Result, AllDocsMapIndex>().CountAsync();
                    Assert.Equal(1, count);
                }

                await store.Maintenance.SendAsync(new DisableIndexOperation(index.IndexName));

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(1), throwOnTimeout: true, indexes: [index.IndexName]);

                    var list = await session.Query<AllDocsMapIndex.Result, AllDocsMapIndex>().ToListAsync();
                    foreach (var company in list)
                    {
                        company.Name = companyName2;
                    }

                    var error = await Assert.ThrowsAsync<RavenTimeoutException>(async() => await session.SaveChangesAsync());
                    Assert.Contains(index.IndexName, error.Message);
                }

                await store.Maintenance.SendAsync(new EnableIndexOperation(index.IndexName));
                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(15));

                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<AllDocsMapIndex.Result, AllDocsMapIndex>()
                        .Where(x => x.Name == companyName2).CountAsync();

                    Assert.Equal(1, count);
                }
            }
        }

        private class AllDocsMapIndex : AbstractIndexCreationTask<object>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                var index = new IndexDefinition
                {
                    Name = IndexName,

                    Maps =
                    {
                        @"
                        from doc in docs
                        select new 
                        {
                            Name = doc.Name
                        }"
                    }
                };

                return index;
            }
        }

    }
}
