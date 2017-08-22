using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.MailingList
{
    public class TransformWithLoad : RavenTestBase
    {
        private void CreateData(IDocumentStore store)
        {
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
        }

        [Fact]
        public void Should_get_id_when_transformer_loads_document()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (var session = store.OpenSession())
                {
                    // Act
                    var contactListViewModel = session.Query<Contact, Contact_ByName>().TransformWith<ContactTransformer, ContactDto>().ToList();

                    // Assert
                    foreach (var detail in contactListViewModel.SelectMany(c => c.ContactDetails))
                    {
                        Assert.NotNull(detail.Id);
                    }

                    var contactViewModel = session.Load<ContactTransformer, ContactDto>("contacts/1");
                    foreach (var detail in contactViewModel.ContactDetails)
                    {
                        Assert.NotNull(detail.Id);
                    }
                }
            }
        }

        [Fact]
        public void LazyLoadById()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
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
        }

        [Fact]
        public void EagerLoadById()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var contactDto = session.Load<ContactTransformer, ContactDto>("contacts/1");
                    foreach (var detail in contactDto.ContactDetails)
                    {
                        Assert.NotNull(detail.Id);
                    }
                }
            }
        }
        [Fact]
        public void WithMetadata()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var c = session.Load<ContactTransformer, ContactDto>("contacts/1");
                    Assert.Equal("hello", c.MetaVal);
                }
            }
        }

        [Fact]
        public void LazyLoadByIds()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.Lazily.Load<ContactTransformer, ContactDto>(new[] { "contacts/1", "contacts/2" }).Value;
                    Assert.Equal(2, result.Count);
                    Assert.NotNull(result["contacts/1"]);
                    Assert.Null(result["contacts/2"]);

                    foreach (var detail in result["contacts/1"].ContactDetails)
                    {
                        Assert.NotNull(detail.Id);
                    }
                }
            }
        }

        [Fact]
        public void LoadByIds()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Load<ContactTransformer, ContactDto>(new[] { "contacts/1", "contacts/2" });
                    Assert.Equal(2, result.Count);
                    Assert.NotNull(result["contacts/1"]);
                    Assert.Null(result["contacts/2"]);

                    foreach (var detail in result["contacts/1"].ContactDetails)
                    {
                        Assert.NotNull(detail.Id);
                    }
                }
            }
        }

        [Fact]
        public void PlainLoadByIds()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);
                using (var session = store.OpenSession())
                {
                    var result = session.Load<Contact>(new[] {"contacts/1", "contacts/2"});
                    Assert.Equal(2, result.Count);
                    Assert.NotNull(result["contacts/1"]);
                    Assert.Null(result["contacts/2"]);
                }
            }
        }

        private class Contact
        {
            public Contact()
            {
                DetailIds = new List<string>();
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public List<string> DetailIds { get; set; }
        }

        private class ContactDto
        {
            public string ContactId { get; set; }
            public string ContactName { get; set; }
            public List<Detail> ContactDetails { get; set; }
            public string MetaVal { get; set; }
        }

        private class ContactTransformer : AbstractTransformerCreationTask<Contact>
        {
            public ContactTransformer()
            {
                TransformResults = contacts => from c in contacts
                                               select new
                                               {
                                                   ContactId = c.Id,
                                                   ContactName = c.Name,
                                                   ContactDetails = LoadDocument<Detail>(c.DetailIds),
                                                   MetaVal = MetadataFor(c).Value<string>("Val")
                                               };
            }
        }

        private class Contact_ByName : AbstractIndexCreationTask<Contact>
        {
            public Contact_ByName()
            {
                Map = contacts => from c in contacts select new { c.Name };

                Index(x => x.Name, FieldIndexing.Search);
            }
        }

        private class Detail
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
