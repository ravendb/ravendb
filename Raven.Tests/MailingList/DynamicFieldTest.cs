using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class DynamicFieldTest : RavenTestBase
    {
        [Fact]
        public void QueryThatCannotBeFulfilledShouldThrow()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item()
                    {
                        SomeNullableDateTime = null
                    });

                    session.Store(new Item()
                    {
                        SomeNullableDateTime = DateTime.UtcNow
                    });

                    session.SaveChanges();
                }

                new ItemsIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var exception = Assert.Throws<ArgumentException>(
                        () => session.Query<Item, ItemsIndex>()
                                     .Count(x => x.SomeNullableDateTime.HasValue));

                    Assert.Equal(
                        "The field 'SomeNullableDateTime' is not indexed, cannot query on fields that are not indexed",
                        exception.Message);
                }
            }
        }

        public class ItemsIndex : AbstractMultiMapIndexCreationTask<Item>
        {
            public ItemsIndex()
            {
                AddMap<Item>(items => from item in items
                                      select new
                                      {
                                          item.Id,
                                          _ = "test"
                                      });
            }
        }

        public class Item
        {
            public string Id { get; set; }

            public DateTime? SomeNullableDateTime { get; set; }
        }
    }
}