using System;
using System.Transactions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs.DTC
{
    public class UsingDTCForUpdates : LocalClientTest
    {
        [Fact]
        public void can_update_a_doc_within_transaction_scope()
        {
            using (var documentStore = NewDocumentStore())
            {
                var id1 = Guid.NewGuid();
                RavenJObject dummy = null;

                using (TransactionScope trnx = new TransactionScope())
                {
                    using (var session = documentStore.OpenSession())
                    {
                        dummy = new RavenJObject();
                        dummy.Add("Name", "This is the object content");
                        dummy.Add("Id", RavenJToken.FromObject(id1));
                        session.Store(dummy);
                        session.SaveChanges();

                    }
                    using (var session = documentStore.OpenSession())
                    {
                        session.Store(dummy);
                        session.SaveChanges();
                    }
                    trnx.Complete();
                }
            }
        }

        [Fact]
        public void can_update_a_doc_after_inserting_another_within_transaction_scope()
        {
            using (var documentStore = NewDocumentStore())
            {
                documentStore.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type, allowNull) => id.ToString();

                var id1 = Guid.NewGuid();
                Foo dummy = null;
                Foo dummy2 = null;

                // 1. First insert the first object
                using (var session = documentStore.OpenSession())
                {
                    dummy = new Foo();
                    dummy.Name = "This is the object content";
                    dummy.Id = id1;
                    session.Store(dummy);
                    session.SaveChanges();
                }

                // 2. Then insert another object and update the first one
                using (TransactionScope trnx = new TransactionScope())
                {
                    using (var session = documentStore.OpenSession())
                    {
                        dummy2 = new Foo();
                        dummy2.Name = "This is the object 2 content";
                        dummy2.Id = Guid.NewGuid();
                        session.Store(dummy2);
                        session.SaveChanges();
                    }

                    using (var session = documentStore.OpenSession())
                    {
                        dummy.Name = "Some other value";
                        session.Store(dummy);
                        session.SaveChanges();
                    }
                    trnx.Complete();
                }
                // 3. Confirm that the update is working !
                using (var session = documentStore.OpenSession())
                {
                	session.Advanced.AllowNonAuthoritiveInformation = false;

                    var stored = session.Load<Foo>(id1.ToString());

                    Assert.Equal(stored.Name , "Some other value");
                }
            }
        }

    }

    public class Foo
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }
}