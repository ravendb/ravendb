using System.Transactions;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class ConcurrencyTests : RavenTest
    {
        [Fact]
        public void With_id_convention_with_transaction()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(store);

                store.Conventions.RegisterIdConvention<Contact>((dbname, commands, contact) => contact.Name);

                using (var session = store.OpenSession())
                {
                    var contact = new Contact
                    {
                        Name = "NameAsId",
                        Title = "Tester"
                    };

                    session.Store(contact);
                    session.SaveChanges();
                }

                using (var transaction = new TransactionScope())
                {
                    using (var session = store.OpenSession())
                    {
                        var contact = session.Load<Contact>("NameAsId");

                        session.Delete(contact);
                        session.SaveChanges();

                        var newContact = new Contact
                        {
                            Name = contact.Name,
                            Title = contact.Title + "Updated"
                        };

                        session.Store(newContact);

                        //This will cause ConcurrencyException: "Transaction operation attempted on : NameAsId using a non current etag"
                        session.SaveChanges();
                    }

                    transaction.Complete();
                }
            }
        }

        [Fact]
        public void With_id_convention_without_transaction()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(store);

                store.Conventions.RegisterIdConvention<Contact>((dbname, commands, contact) => contact.Name);

                using (var session = store.OpenSession())
                {
                    var contact = new Contact
                    {
                        Name = "NameAsId",
                        Title = "Tester"
                    };

                    session.Store(contact);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var contact = session.Load<Contact>("NameAsId");

                    session.Delete(contact);
                    session.SaveChanges();

                    var newContact = new Contact
                    {
                        Name = contact.Name,
                        Title = contact.Title + " Updated"
                    };

                    session.Store(newContact);

                    //This works!
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void Without_id_convention_with_transaction()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(store);

                using (var session = store.OpenSession())
                {
                    var contact = new Category
                    {
                        Id = "categories/1",
                        Name = "Test category"
                    };

                    session.Store(contact);
                    session.SaveChanges();
                }

                using (var transaction = new TransactionScope())
                {
                    using (var session = store.OpenSession())
                    {
                        var category = session.Load<Category>("categories/1");

                        session.Delete(category);
                        session.SaveChanges();

                        var newCategory = new Category
                        {
                            Id = category.Id,
                            Name = category.Name + "Updated"
                        };

                        session.Store(newCategory);

                        //This works!
                        session.SaveChanges();
                    }

                    transaction.Complete();
                }
            }
        }

        public class Contact
        {
            public string Name { get; set; }
            public string Title { get; set; }
        }

        public class Category
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }
    }
}