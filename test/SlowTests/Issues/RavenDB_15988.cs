using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15988 : RavenTestBase
    {
        public RavenDB_15988(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Use_FindIndex_In_Index()
        {
            using (var store = GetDocumentStore())
            {
                await store.ExecuteIndexAsync(new TestIndex());

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TestObj
                    {
                        List = new List<int>
                        {
                            3,
                            4,
                            5,
                            6,
                            1,
                            2,
                            1,
                            3,
                            3
                        }
                    });

                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var terms = await store.Maintenance.SendAsync(new GetTermsOperation(new TestIndex().IndexName, "IndexFirst", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("4", terms[0]);

                terms = await store.Maintenance.SendAsync(new GetTermsOperation(new TestIndex().IndexName, "IndexLast", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("6", terms[0]);
            }
        }

        private class TestIndex : AbstractIndexCreationTask<TestObj, TestIndex.Result>
        {
            public class Result : TestObj
            {
                public int IndexFirst { get; set; }

                public int IndexLast { get; set; }
            }

            public TestIndex()
            {
                Map = products =>
                    from product in products
                    let i = product.List.FindIndex(d => d == 1)
                    let j = product.List.FindLastIndex(d => d == 1)
                    select new Result
                    {
                        IndexFirst = i,
                        IndexLast = j
                    };
            }
        }

        private class TestObj
        {
            public string Id { get; set; }
            public List<int> List { get; set; }
        }
    }
}
