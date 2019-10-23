using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_12946 : RavenTestBase
    {
        public RavenDB_12946(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Raven_Should_Support_Include_More_Than_Once_On_Same_Doc()
        {
            const int numberOfDocs = 335;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var item = new User2()
                    {
                        Id = "Users/1",
                        Collection = new List<BookReference>()
                    };

                    var lookup = new Lookup()
                    {
                        Id = "Lookup/User/ByUsername/Kamranicus",
                        ReferenceId = "Users/1"
                    };

                    for (var i = 0; i < numberOfDocs; i++)
                    {
                        item.Collection.Add(new BookReference()
                        {
                            BookId = $"Books/{i}"
                        });
                        await session.StoreAsync(new Book()
                        {
                            Id = $"Books/{i}"
                        });
                    }

                    await session.StoreAsync(lookup);
                    await session.StoreAsync(item);
                    await session.SaveChangesAsync();
                }
               
                using (var session = store.OpenSession())
                {
                    // Set up a Include in the session
                    var lookupByUsername = session.Include<Lookup>(l => l.ReferenceId)
                        .Load<Lookup>("Lookup/User/ByUsername/Kamranicus");

                    // Load the document once
                    var user = session.Load<User2>(lookupByUsername.ReferenceId);

                    // Then set up a second Include in the session
                    // and load the same document
                    var item = session
                        .Include<User2, Book>(i => i.Collection.Select(g => g.BookId))
                        .Load<User2>(user.Id);

                    var list = new List<Book>();
                    foreach (var bookRef in item.Collection)
                    {
                        list.Add(session.Load<Book>(bookRef.BookId));
                    }

                    Assert.Equal(335, list.Count);
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }
    }

    public class Book
    {
        public string Id { get; set; }
    }

    public class BookReference
    {
        public string BookId { get; set; }
    }

    public class User2
    {
        public string Id { get; set; }
        public List<BookReference> Collection { get; set; }
    }

    public class Lookup
    {
        public string Id { get; set; }

        public string ReferenceId { get; set; }
    }
}
