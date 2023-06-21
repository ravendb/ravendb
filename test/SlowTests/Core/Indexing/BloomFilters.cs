// -----------------------------------------------------------------------
//  <copyright file="CustomAnalyzers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.Indexing
{
    public class BloomFilters : RavenTestBase
    {
        public BloomFilters(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Skip_Storing_In_Bloom_Filter_If_No_Document_Was_Stored_In_Lucene()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Age = 37
                        }, i.ToString());
                    }
                    
                    await session.SaveChangesAsync();
                }

                await new Index().ExecuteAsync(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var resultsCount = await session.Query<User, Index>().CountAsync();
                    Assert.Equal(0, resultsCount);
                }

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("Index");

                using (var context = new TransactionOperationContext(index._environment, 1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
                {
                    using (context.OpenWriteTransaction())
                    {
                        using (var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                        {
                            Assert.Equal(1, collection.Count);
                            Assert.Equal(0, collection.CurrentFilterCount);
                        }
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Age = 38
                        }, i.ToString());
                    }

                    await session.SaveChangesAsync();
                }

                using (var context = new TransactionOperationContext(index._environment, 1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
                {
                    using (context.OpenWriteTransaction())
                    {
                        using (var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                        {
                            Assert.Equal(1, collection.Count);
                            Assert.Equal(10, collection.CurrentFilterCount);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task Delete_From_Index_If_The_Document_Isnt_Relevant_For_The_Index_Anymore()
        {
            using (var store = GetDocumentStore())
            {
                await new Index().ExecuteAsync(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Age = 38
                    }, "test");
                    await session.SaveChangesAsync();
                }

                await new Index().ExecuteAsync(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var resultsCount = await session.Query<User, Index>().CountAsync();
                    Assert.Equal(1, resultsCount);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Age = 37
                    }, "test");
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var resultsCount = await session.Query<User, Index>().CountAsync();
                    Assert.Equal(0, resultsCount);
                }
            }
        }

        private class Index : AbstractIndexCreationTask<User>
        {
            public Index()
            {
                Map = users => from user in users
                               where user.Age != 37
                                select new
                                {
                                    user.Age
                                };
            }
        }
    }
}
