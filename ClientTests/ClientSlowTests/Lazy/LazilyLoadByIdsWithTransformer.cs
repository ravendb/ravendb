using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexes;
using Xunit;


namespace NewClientTests.NewClient
{
    public class LazilyLoadByIdsWithTransformer : RavenTestBase
    {
        [Fact]
        public void LazyLoadById()
        {
            var store = GetDocumentStore();
            new ContactTransformer().Execute(store);
            new Contact_ByName().Execute(store);

            using (var session = store.OpenSession())
            {
                var detail1 = new Detail
                {
                    Name = "Detail 1",
                };
                var detail2 = new Detail
                {
                    Name = "Detail 2"
                };
                session.Store(detail1);
                session.Store(detail2);
                session.SaveChanges();


                var contact = new Contact
                {
                    Id = "contacts/1",
                    Name = "Contact 1",
                    DetailIds = new List<string>
                    {
                        detail1.Id,
                        detail2.Id
                    }
                };
                session.Store(contact);
                session.Advanced.GetMetadataFor(contact)["Val"] = "hello";
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var contactViewModel = session.Advanced.Lazily.Load<ContactTransformer, ContactDto>("contacts/1");
                var contactDto = contactViewModel.Value;
                foreach (var detail in contactDto.ContactDetails)
                {
                    Assert.NotNull(detail.Id);
                }
            }
        }

        public class ContactDto
        {
            public string ContactId { get; set; }
            public string ContactName { get; set; }
            public List<Detail> ContactDetails { get; set; }
            public string MetaVal { get; set; }
        }

        public class ContactTransformer : AbstractTransformerCreationTask<Contact>
        {
            public ContactTransformer()
            {
                TransformResults = contacts => from c in contacts
                                               select new
                                               {
                                                   ContactId = c.Id,
                                                   ContactName = c.Name,
                                                   ContactDetails = LoadDocument<Detail>(c.DetailIds)
                                               };
            }
        }

        public class Contact_ByName : AbstractIndexCreationTask<Contact>
        {
            public Contact_ByName()
            {
                Map = contacts => from c in contacts select new { c.Name };

                Index(x => x.Name, FieldIndexing.Analyzed);
            }
        }

        public class Detail
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class Contact
        {
            public Contact()
            {
                DetailIds = new List<string>();
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public List<string> DetailIds { get; set; }
        }

        [Fact]
        public void WithTransformer()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new ItemsTransformer());

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Position = 1 });
                    session.Store(new Item { Position = 2 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Load<ItemsTransformer, Item>(new[] { "items/1", "items/2" });
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Advanced.Lazily.Load<ItemsTransformer, Item>(new[] { "items/1", "items/2" }).Value;
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }
            }
        }

        [Fact]
        public void WithTransformer2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new ItemsTransformer());

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Position = 1 });
                    session.Store(new Item { Position = 2 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Load<Item>(new[] { "items/1", "items/2" }, typeof(ItemsTransformer));
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Advanced.Lazily.Load<Item>(new[] { "items/1", "items/2" }, typeof(ItemsTransformer)).Value;
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }
            }
        }

        private class Item
        {
            public int Position { get; set; }
        }

        private class ItemsTransformer : AbstractTransformerCreationTask<Item>
        {
            public ItemsTransformer()
            {
                TransformResults = docs => docs.Select(doc => new { Position = doc.Position * 3 });
            }
        }

    }
}
