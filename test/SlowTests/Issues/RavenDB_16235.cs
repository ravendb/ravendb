using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16235 : RavenTestBase
    {
        public RavenDB_16235(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(1)]
        [InlineData(LazyStringReader.MinLengthForLazyStringStreamReader)]
        public void Exceeding_The_Field_Cache_Size_Should_Not_Cause_Indexing_Issues_With_Disposed_Reader(int length)
        {
            using (var store = GetDocumentStore())
            {
                var data = new string('a', length);

                using (var session = store.OpenSession())
                {
                    var item = new Item();
                    for (var i = 0; i < LuceneDocumentConverterBase.MaximumNumberOfItemsInFieldsCacheForMultipleItemsSameField + 1; i++)
                    {
                        item.Items.Add(data);
                    }

                    session.Store(item);
                    session.SaveChanges();
                }

                new Items_ByItems().Execute(store);

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        private class Items_ByItems : AbstractIndexCreationTask<Item>
        {
            public Items_ByItems()
            {
                Map = items => from item in items
                               select new
                               {
                                   item.Items
                               };
            }
        }

        private class Item
        {
            public List<string> Items { get; set; } = new List<string>();
        }
    }
}
