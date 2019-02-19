using System.Collections.Generic;
using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class Viktor : RavenTestBase
    {
        [Fact]
        public void CanQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TestDocumentByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument { Name = "Hello world!" });
                    session.Store(new TestDocument { Name = "Goodbye..." });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var items = session.Query<TestDocument>().ToList();
                
                    var query = from item in session.Query<TestDocument, TestDocumentByName>()
                                select new
                                {
                                    Name = "XYZ: " + item.Name,
                                    Key = item.Id.Split("/".ToCharArray()).Last()
                                };

                    Assert.True(query.Any());
                }
            }
        }

        [Fact]
        public void CanQuery2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TestDocumentByName());
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument
                    {
                        Name = "Hello world!",
                        Docs = new Dictionary<int, TestDocument.InfoDoc> { { 1, new TestDocument.InfoDoc { SomeValue = "A" } } }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = from item in session.Query<TestDocument, TestDocumentByName>()
                                select new
                                {
                                    Name = "XYZ: " + item.Name,
                                    Docs = item.Docs.Select(doc => new { Val = doc.Value.SomeValue })
                                };

                    Assert.True(query.Any());
                }
            }
        }

        [Fact]
        public void CanUseToCharArrayInsideProjection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument { Name = "Hello world!" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from item in session.Query<TestDocument>()
                                select new
                                {
                                    Chars1 = item.Name.ToCharArray(),
                                    Chars2 = item.Name.ToCharArray(3, 7)
                                };

                    var result = query.ToList();
                    Assert.Equal("Hello world!".ToCharArray(), result[0].Chars1);
                    Assert.Equal("Hello world!".ToCharArray(3, 7), result[0].Chars2);
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
            public IDictionary<int, InfoDoc> Docs { get; set; } = new Dictionary<int, InfoDoc>();

            public class InfoDoc
            {
                public string SomeValue { get; set; }
            }
        }
    }
}
