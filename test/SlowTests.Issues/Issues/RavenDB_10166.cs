using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10166:RavenTestBase
    {
        public RavenDB_10166(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public async Task RandomResultsShouldBeRecievedInDifferentOrderEachTime(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Vasilii",
                        Age = 1
                    });
                    await session.StoreAsync(new User
                    {
                        Name = "Ivan",
                        Age = 1
                    });
                    await session.StoreAsync(new User
                    {
                        Name = "Michail",
                        Age = 1
                    });
                    await session.StoreAsync(new User
                    {
                        Name = "Danil",
                        Age = 2
                    });
                    await session.StoreAsync(new User
                    {
                        Name = "Lazar",
                        Age = 3
                    });

                    await session.SaveChangesAsync();
                }

                List<User> firstOrder;
                using (var session = store.OpenAsyncSession())
                {
                    firstOrder = await session.Advanced.AsyncDocumentQuery<User>()
                        .WaitForNonStaleResults()
                        .OrderBy(x => x.Age)
                        .RandomOrdering().ToListAsync();
                }

                bool orderVaried = false;
                for (var i=0; i< 10; i++)
                {
                    store.GetRequestExecutor().Cache.Clear();
                    using (var session = store.OpenAsyncSession())
                    {
                        var curOrder = await session.Advanced.AsyncDocumentQuery<User>()
                            .WaitForNonStaleResults()
                            .OrderBy(x => x.Age)
                            .RandomOrdering().ToListAsync();
                        for (var docIndex=0; docIndex < curOrder.Count; docIndex++)
                        {
                            if (curOrder[docIndex].Name != firstOrder[docIndex].Name)
                            {
                                orderVaried = true;
                                break;
                            }
                        }
                    }
                }
                Assert.True(orderVaried);
            }
        }
    }
}
