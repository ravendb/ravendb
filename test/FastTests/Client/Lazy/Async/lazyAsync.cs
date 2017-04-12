using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Lazy.Async
{
    public class LazyAsync : RavenTestBase
    {
        [Fact]
        public async Task CanLazilyLoadEntity()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";
            const string COMPANY3_ID = "companies/3";
            const string COMPANY4_ID = "companies/4";
            const string COMPANY5_ID = "companies/5";
            const string COMPANY6_ID = "companies/6";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Id = COMPANY1_ID }, COMPANY1_ID);
                    session.Store(new Company { Id = COMPANY2_ID }, COMPANY2_ID);
                    session.Store(new Company { Id = COMPANY3_ID }, COMPANY3_ID);
                    session.Store(new Company { Id = COMPANY4_ID }, COMPANY4_ID);
                    session.Store(new Company { Id = COMPANY5_ID }, COMPANY5_ID);
                    session.Store(new Company { Id = COMPANY6_ID }, COMPANY6_ID);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var lazyOrder = session.Advanced.Lazily.LoadAsync<Company>(COMPANY1_ID);
                    Assert.False(lazyOrder.IsValueCreated);
                    var order = await lazyOrder.Value;
                    Assert.Equal(COMPANY1_ID, order.Id);

                    var lazyOrders = session.Advanced.Lazily.LoadAsync<Company>(new string[] { COMPANY1_ID, COMPANY2_ID });
                    Assert.False(lazyOrders.IsValueCreated);
                    var orders = await lazyOrders.Value;
                    Assert.Equal(2, orders.Count);
                    Company company1;
                    Company company2;
                    orders.TryGetValue(COMPANY1_ID, out company1);
                    orders.TryGetValue(COMPANY2_ID, out company2);

                    Assert.NotNull(company1);
                    Assert.NotNull(company2);
                    Assert.Equal(COMPANY1_ID, company1.Id);
                    Assert.Equal(COMPANY2_ID, company2.Id);

                    lazyOrder = session.Advanced.Lazily.LoadAsync<Company>("companies/3");
                    Assert.False(lazyOrder.IsValueCreated);
                    order = await lazyOrder.Value;
                    Assert.Equal(COMPANY3_ID, order.Id);

                    lazyOrders = session.Advanced.Lazily.LoadAsync<Company>(new[] { "4", "5" });
                    Assert.False(lazyOrders.IsValueCreated);
                    orders = await lazyOrders.Value;
                    Assert.Equal(2, orders.Count);
                    orders.TryGetValue(COMPANY4_ID, out company1);
                    orders.TryGetValue(COMPANY5_ID, out company2);
                }
            }
        }

        [Fact]
        public async Task CanExecuteAllPendingLazyOperations()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Id = COMPANY1_ID }, COMPANY1_ID);
                    session.Store(new Company { Id = COMPANY2_ID }, COMPANY2_ID);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    Company company1 = null;
                    Company company2 = null;

                    session.Advanced.Lazily.LoadAsync<Company>(COMPANY1_ID, x => company1 = x);
                    session.Advanced.Lazily.LoadAsync<Company>(COMPANY2_ID, x => company2 = x);
                    Assert.Null(company1);
                    Assert.Null(company2);

                    await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
                    Assert.NotNull(company1);
                    Assert.NotNull(company2);
                    Assert.Equal(COMPANY1_ID, company1.Id);
                    Assert.Equal(COMPANY2_ID, company2.Id);
                }
            }
        }

        [Fact]
        public async void WithQueuedActions_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    User user = null;
                    session.Advanced.Lazily.LoadAsync<User>("users/1", x => user = x);
                    await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();
                    Assert.NotNull(user);
                }

            }
        }

        [Fact]
        public async Task LazyLoadById()
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

            using (var session = store.OpenAsyncSession())
            {
                var contactViewModel = session.Advanced.Lazily.LoadAsync<ContactTransformer, ContactDto>("contacts/1");
                var contactDto = await contactViewModel.Value;
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
        public async Task WithTransformer()
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

                using (var session = store.OpenAsyncSession())
                {
                    var items = await session.LoadAsync<ItemsTransformer, Item>(new[] { "items/1", "items/2" });
                    Assert.Equal(1 * 3, items["items/1"].Position);
                    Assert.Equal(2 * 3, items["items/2"].Position);
                }
            }
        }

        [Fact]
        public async Task WithTransformer2()
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

                using (var session = store.OpenAsyncSession())
                {
                    var items = await session.LoadAsync<Item>(new[] { "items/1", "items/2" }, typeof(ItemsTransformer));
                    Assert.Equal(1 * 3, items["items/1"].Position);
                    Assert.Equal(2 * 3, items["items/2"].Position);
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
