using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Victor : RavenTestBase
    {
        private class Item
        {
            public string Id { get; set; }
            public Attribute[] Attributes { get; set; }
        }

        private class Attribute
        {
            public Attribute(string name, decimal value)
            {
                Name = name;
                Value = value;
            }

            public string Name { get; set; }
            public decimal Value { get; set; }
        }

        private class WithDynamicIndex : AbstractIndexCreationTask<Item>
        {
            public WithDynamicIndex()
            {
                Map = items =>
                      from item in items
                      select new
                      {
                        _ = item.Attributes.Select(x => CreateField(x.Name, x.Value))
                      };
            }
        }

        [Fact(Skip = "Missing feature: CreateField")]
        public void CanSortDynamically()
        {
            using (var store = GetDocumentStore())
            {
                new WithDynamicIndex().Execute(store);
                using (var s = store.OpenSession())
                {
                    s.Store(new Item
                    {
                        Attributes = new[] { new Attribute("T1", 10.99m) }
                    });
                    s.Store(new Item
                    {
                        Attributes = new[] { new Attribute("T1", 11.99m) }
                    });
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    var items = s.Advanced.DocumentQuery<Item, WithDynamicIndex>()
                        .WaitForNonStaleResults()
                        .OrderBy("-T1") //System.ArgumentException: The field 'T1' is not indexed, cannot sort on fields that are not indexed
                        .ToList();
                    Assert.Equal(2, items.Count);
                }
            }
        }

    }
}
