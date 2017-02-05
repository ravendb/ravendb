using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Database.Storage;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_6238: RavenTest
    {

        [Fact]
        public async Task SimplMapIndexesWorkNotificationAmountTest_SingleItemBatchesTest()
        {
            using (var store = NewDocumentStore(configureStore: x =>
            {
                x.Configuration.MaxNumberOfItemsToProcessInSingleBatch = 1;
                x.Configuration.MaxNumberOfItemsToReduceInSingleBatch = 1;
                x.Configuration.InitialNumberOfItemsToProcessInSingleBatch = 1;
            }))
            {
                var indexesAmount = 20;
                var documentsAmount = 20;
                for (var i = 0; i < indexesAmount; i++)
                {
                    await store.AsyncDatabaseCommands.PutIndexAsync("Index" + i, new IndexDefinition()
                    {
                        Map = @"from user in docs.Users
                                select new {
                                            user.Name
                                           }"
                    }).ConfigureAwait(false);
                }

                var server = store.ServerIfEmbedded;
                var dd = await server.Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase);

                WaitForIndexing(store);
                var workCountBeforeInsert = dd.WorkContext.GetWorkCount();
                

                using (var session = store.OpenAsyncSession())
                {
                    
                    for (int i = 0; i < documentsAmount; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "User_" + i
                        },"users/"+(i+1)).ConfigureAwait(false);
                    }
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                WaitForIndexing(store);

                var workCountAfterIndexingEnded = dd.WorkContext.GetWorkCount();

                Assert.True(workCountAfterIndexingEnded - workCountBeforeInsert < indexesAmount * documentsAmount /2);
            }
        }


        [Fact]
        public async Task SimplMapIndexesWorkNotificationAmountTest()
        {
            using (var store = NewDocumentStore())
            {
                var indexesAmount = 20;
                var documentsAmount = 20;
                for (var i = 0; i < indexesAmount; i++)
                {
                    await store.AsyncDatabaseCommands.PutIndexAsync("Index" + i, new IndexDefinition()
                    {
                        Map = @"from user in docs.Users
                                select new {
                                            user.Name
                                           }"
                    }).ConfigureAwait(false);
                }

                var server = store.ServerIfEmbedded;
                var dd = await server.Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase);

                WaitForIndexing(store);
                var workCountBeforeInsert = dd.WorkContext.GetWorkCount();


                using (var session = store.OpenAsyncSession())
                {

                    for (int i = 0; i < documentsAmount; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "User_" + i
                        }, "users/" + (i + 1)).ConfigureAwait(false);
                    }
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                WaitForIndexing(store);

                var workCountAfterIndexingEnded = dd.WorkContext.GetWorkCount();

                Assert.True(workCountAfterIndexingEnded - workCountBeforeInsert < indexesAmount * documentsAmount / 2);
            }
        }


        [Fact]
        public async Task ReduceIndexesWorkNotificationAmountTest_SingleItemBatchesTest()
        {
            using (var store = NewDocumentStore(configureStore: x =>
            {
                x.Configuration.MaxNumberOfItemsToProcessInSingleBatch = 1;
                x.Configuration.MaxNumberOfItemsToReduceInSingleBatch = 1;
                x.Configuration.InitialNumberOfItemsToProcessInSingleBatch = 1;
            }))
            {
                var indexesAmount = 20;
                var documentsAmount = 20;
                for (var i = 0; i < indexesAmount; i++)
                {
                    await store.AsyncDatabaseCommands.PutIndexAsync("Index" + i, new IndexDefinition()
                    {
                        Map = @"from user in docs.Users
                                select new {
                                            user.Name,
                                            user.Active,
                                            Count = 1
                                           }",
                        Reduce = @"from result in results
                                   group result by result.Active into g
                                    select new {
                                        Name = g.First().Name,
                                        Active = g.First().Active,
                                        Count = g.Sum(x=>x.Count)
                                    }"
                    }).ConfigureAwait(false);
                }

                var server = store.ServerIfEmbedded;
                var dd = await server.Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase);
                
                var workCountBeforeInsert = dd.WorkContext.GetWorkCount();
                using (var session = store.OpenAsyncSession())
                {
                    
                    for (int i = 0; i < documentsAmount; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "User_" + i,
                            Active = i%2==0
                        }, "users/" + (i + 1)).ConfigureAwait(false);
                    }
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                WaitForIndexing(store);

                var workCountAfterIndexingEnded = dd.WorkContext.GetWorkCount();

                Assert.True(workCountAfterIndexingEnded - workCountBeforeInsert < indexesAmount * documentsAmount / 2);
            }
        }

        [Fact]
        public async Task ReduceIndexesWorkNotificationAmountTest()
        {
            using (var store = NewDocumentStore())
            {
                var indexesAmount = 20;
                var documentsAmount = 20;
                for (var i = 0; i < indexesAmount; i++)
                {
                    await store.AsyncDatabaseCommands.PutIndexAsync("Index" + i, new IndexDefinition()
                    {
                        Map = @"from user in docs.Users
                                select new {
                                            user.Name,
                                            user.Active,
                                            Count = 1
                                           }",
                        Reduce = @"from result in results
                                   group result by result.Active into g
                                    select new {
                                        Name = g.First().Name,
                                        Active = g.First().Active,
                                        Count = g.Sum(x=>x.Count)
                                    }"
                    }).ConfigureAwait(false);
                }

                var server = store.ServerIfEmbedded;
                var dd = await server.Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase);

                var workCountBeforeInsert = dd.WorkContext.GetWorkCount();
                using (var session = store.OpenAsyncSession())
                {

                    for (int i = 0; i < documentsAmount; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "User_" + i,
                            Active = i % 2 == 0
                        }, "users/" + (i + 1)).ConfigureAwait(false);
                    }
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                WaitForIndexing(store);

                var workCountAfterIndexingEnded = dd.WorkContext.GetWorkCount();

                Assert.True(workCountAfterIndexingEnded - workCountBeforeInsert < indexesAmount * documentsAmount / 2);
            }
        }
    }
}
