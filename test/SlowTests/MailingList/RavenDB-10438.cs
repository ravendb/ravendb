using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;
namespace SlowTests.MailingList
{
    public class RavenDB_10438 : RavenTestBase
    {
        public RavenDB_10438(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQuery2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TestDocumentByName());

                using (var session = store.OpenSession())
                {
                    session.Store(new Category
                    {
                        Id = "categories/1",
                        Name = new Dictionary<string, string>
                    {
                        { "EN","category"},
                        { "DE","Kategorie"},
                        { "UA","kатегорія"}
                    }
                    });
                    session.Store(new TestDocument
                    {
                        Name = "TEST",
                        Categories = new List<string> { { "categories/1" } }
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var param = "A";
                    var query = from item in session.Query<TestDocument, TestDocumentByName>()
                                let categories = RavenQuery.Load<Category>(item.Categories)
                                select new
                                {
                                    Key = item.Id.Split("/".ToCharArray()).Last(),
                                    Name = item.Name,
                                    Nested = from cat in categories
                                             let name = cat.Name[param]
                                             select new
                                             {
                                                 Key = cat.Id.Split("/".ToCharArray()).Last(),
                                                 Name = name
                                             }
                                };

                    Assert.True(query.Any());
                }
            }
        }
        [Fact]
        public void CanQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TestDocumentByName());

                using (var session = store.OpenSession())
                {
                    session.Store(new Category
                    {
                        Id = "categories/1",
                        Name = new Dictionary<string, string>
                    {
                        { "EN","category"},
                        { "DE","Kategorie"},
                        { "UA","kатегорія"}
                    }
                    });
                    session.Store(new TestDocument
                    {
                        Name = "TEST",
                        Categories = new List<string> { { "categories/1" } }
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var param = "A";

                    var query = from item in session.Query<TestDocument>()
                                let categories = RavenQuery.Load<Category>(item.Categories)
                                select new
                                {
                                    Name = item.Name,
                                    Nested = from cat in categories
                                             let name = cat.Name[param]
                                             let name2 = cat.Name[param]
                                             select new
                                             {
                                                 Name = name
                                             }
                                };

                    Assert.True(query.Any());
                }
            }
        }

        public class TestDocumentByName : AbstractIndexCreationTask<TestDocument>
        {
            public TestDocumentByName()
            {
                Map = docs => from doc in docs select new { doc.Name };
                Indexes.Add(x => x.Name, FieldIndexing.Search);
            }
        }

        public class TestDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public IList<string> Categories { get; set; }
        }

        public class Category
        {
            public string Id { get; set; }
            public IDictionary<string, string> Name { get; set; } = new Dictionary<string, string>();
        }
    }
}
