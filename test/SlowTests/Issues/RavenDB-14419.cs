using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Subscriptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14419 : RavenTestBase
    {
        public RavenDB_14419(ITestOutputHelper output) : base(output)
        {
        }

        public class Item
        {
            public string Name { get; set; }
            public Item Son { get; set; }
        }

        public class Index : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Name = "Index",
                    Maps =
                    {
                        @"
from item in docs.Items
select new
{
    Name = item?.Son?.Name ?? item.Name ?? ""Unnamed""
}"
                    }
                };
            }
        }

        [Fact]
        public void CanUseNullConditionAccessAndNullCoalescingTogether()
        {
            using (var store = GetDocumentStore())
            {
                new Index().Execute(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new Item
                    {
                        Name = "one"
                    });
                    s.Store(new Item
                    {
                        Son = new Item
                        {
                            Name = "two"
                        }
                    });
                    s.Store(new Item());
                    s.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    Assert.Equal(1, 
                        s.Query<Item, Index>().Count(x=>x.Name == "Unnamed"));
                    Assert.Equal(1,
                        s.Query<Item, Index>().Count(x => x.Name == "One"));
                    Assert.Equal(1,
                        s.Query<Item, Index>().Count(x => x.Name == "Two"));
                }
            }
        }
    }
}
