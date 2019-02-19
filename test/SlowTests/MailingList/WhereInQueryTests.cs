using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class WhereInQueryTests : RavenTestBase
    {
        private void CreateData(IDocumentStore store)
        {
            using (IDocumentSession session = store.OpenSession())
            {
                session.Store(new TestDocument
                {
                    Id = "Doc1",
                    FirstName = "FirstName1",
                    LastName = "LastName1"
                });
                session.Store(new TestDocument
                {
                    Id = "Doc2",
                    FirstName = "FirstName2",
                    LastName = "LastName2"
                });
                session.Store(new TestDocument
                {
                    Id = "Doc3",
                    FirstName = "FirstName3",
                    LastName = "LastName3"
                });
                session.SaveChanges();
            }

            new TestIndex().Execute(store);
        }

        [Fact]
        public void Query_should_return_2_results()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    List<TestDocument> results = session
                        .Advanced
                        .DocumentQuery<TestDocument, TestIndex>()
                        .WhereEquals("FirstName", "FirstName1")
                        .OrElse()
                        .WhereEquals("LastName", "LastName2")
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }

        [Fact]
        public void Query_using_WhereIn_should_return_2_results()
        {
            using (var store = GetDocumentStore())
            {
                CreateData(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    List<TestDocument> results = session
                        .Advanced
                        .DocumentQuery<TestDocument, TestIndex>()
                        .WhereIn("FirstName", new[] { "FirstName1" })
                        .OrElse()
                        .WhereIn("LastName", new[] { "LastName2" })
                        .WaitForNonStaleResults()
                        .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }

        private class TestDocument
        {
            public string FirstName { get; set; }

            public string Id { get; set; }

            public string LastName { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<TestDocument>
        {
            public TestIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.FirstName,
                                  doc.LastName
                              };
            }
        }
    }
}
