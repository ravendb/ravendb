using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class RavenDbPutTest : RavenTestBase
    {
        public class Book
        {
            public string Id { get; set; }
            public Guid OldId { get; set; }
            public string Name { get; set; }

        }

        [Fact]
        public void strangely_puts_after_just_a_query()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();

                using (var session = store.OpenSession())
                {
                    session.Store(new Book { Name = "Hello", });
                    session.Store(new Book { Name = "Baby", });
                    session.Store(new Book { Name = "Deer", });

                    session.SaveChanges();
                }

                // Now try querying the index and see the strange PUT requests.
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Book>()
                        .Customize(x => x.WaitForNonStaleResults());

                    var results = query.ToList();

                    var old = session.Advanced.NumberOfRequests;
                    session.SaveChanges();
                    Assert.Equal(old, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
