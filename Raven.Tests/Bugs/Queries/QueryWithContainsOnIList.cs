using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Bugs.Queries
{
    public class QueryWithContainsOnIList : RavenTest
    {
        [Fact]
        public void CanQueryWithContainsOnIList()
        {
            using (var store = NewDocumentStore())
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
