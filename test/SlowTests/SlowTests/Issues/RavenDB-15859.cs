using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_15859 : RavenTestBase
    {
        public RavenDB_15859(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task QueryShouldEnd()
        {
            using var store = GetDocumentStore();
            await store.Maintenance.SendAsync(new PutIndexesOperation(new TestIndex().CreateIndexDefinition()));

            var rnd = new Random(123);

            var documents = Enumerable.Range(0, 200)
                .Select(i => new TestItem
                {
                    BoolProp = rnd.Next(3) == 0,
                    StringProp = Guid.NewGuid().ToString()
                })
                .ToList();

            using (var s1 = store.OpenAsyncSession())
            {
                s1.Advanced.WaitForIndexesAfterSaveChanges();
                foreach (var document in documents)
                {
                    await s1.StoreAsync(document);
                }

                await s1.SaveChangesAsync();
            }
            var filter = documents.Select(d => d.StringProp).ToArray();

            using (var s2 = store.OpenAsyncSession())
            {
                var result = await s2.Advanced
                    .AsyncDocumentQuery<TestItem, TestIndex>()
                    .WhereEquals(x => x.BoolProp, false) // if we don't use this condition, query also works
                    .AndAlso()
                    // .WhereIn(x => x.StringProp, filter.Take(128).ToArray()) // this works
                    .WhereIn(x => x.StringProp, filter.Take(129).ToArray()) // this doesn't
                    .ToListAsync();
            }

        }


        private class TestItem
        {
            public string Id, StringProp;
            public bool BoolProp;
        }

        private class TestIndex : AbstractIndexCreationTask<TestItem>
        {
            public TestIndex()
            {
                Map = docs => from doc in docs
                    select new
                    {
                        Id = doc.Id,
                        doc.StringProp,
                        doc.BoolProp
                    };
            }
        }
    }
}
