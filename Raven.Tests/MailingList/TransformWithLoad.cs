using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class TransformWithLoad : RavenTestBase
    {
        [Fact]
        public void Should_get_id_when_transformer_loads_document()
        {
            using (var store = NewDocumentStore())
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
                        Name = "Contact 1",
                        DetailIds = new List<string>
                        {
                            detail1.Id,
                            detail2.Id
                        }
                    };

                    session.Store(contact);
                    session.SaveChanges();

                    // Act
                    var contactListViewModel = session.Query<Contact, Contact_ByName>().TransformWith<ContactTransformer, ContactDto>().ToList();

                    // Assert
                    foreach (var detail in contactListViewModel.SelectMany(c => c.ContactDetails))
                    {
                        Assert.NotNull(detail.Id);
                    }

                    var contactViewModel = session.Load<ContactTransformer, ContactDto>(contact.Id);
                    foreach (var detail in contactViewModel.ContactDetails)
                    {
                        Assert.NotNull(detail.Id);
                    }
                }
            }
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

        public class ContactDto
        {
            public string ContactId { get; set; }
            public string ContactName { get; set; }
            public List<Detail> ContactDetails { get; set; }
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
    }
}