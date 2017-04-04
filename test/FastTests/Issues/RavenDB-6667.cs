using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_6667 : RavenTestBase
    {
        [Fact]
        public void CanQueryWithContainsOnIList()
        {
            using (var store = GetDocumentStore())
            {
                new Authors_ByNameAndBooks().Execute(store);

                using (var session = store.OpenSession())
                {
                    IList<Author> results = session
                        .Query<Authors_ByNameAndBooks.Result, Authors_ByNameAndBooks>()
                        .Where(x => x.Name == "Andrzej Sapkowski" || x.Books.Contains("The Witcher"))
                        .OfType<Author>()
                        .ToList();
                }
            }
        }

        public class Book
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        public class Author
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public IList<string> BookIds { get; set; }
        }

        public class Authors_ByNameAndBooks : AbstractIndexCreationTask<Author>
        {
            public class Result
            {
                public string Name { get; set; }

                public IList<string> Books { get; set; }
            }

            public Authors_ByNameAndBooks()
            {
                Map = authors => from author in authors
                    select new
                    {
                        Name = author.Name,
                        Books = author.BookIds.Select(x => LoadDocument<Book>(x).Name)
                    };
            }
        }
    }
}
