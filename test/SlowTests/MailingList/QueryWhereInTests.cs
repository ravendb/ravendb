using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class QueryWhereInTests : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Document_Index : AbstractIndexCreationTask<Document>
        {
            public Document_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  doc.Name,
                              };
            }
        }

        private void SetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(new Document
                {
                    Id = "documents/1",
                    Name = "Doc1",
                });


                session.Store(new Document
                {
                    Id = "documents/3",
                    Name = "Doc3",
                });

                session.Store(new Document
                {
                    Id = "documents/5",
                    Name = "Doc5",
                });

                session.SaveChanges();
            }

            WaitForIndexing(store);
        }

        private void AssertMatches(IDocumentStore store, IEnumerable<string> idsToMatch, int expectedCount)
        {
            using (var session = store.OpenSession())
            {
                var queryCount = session.Query<Document, Document_Index>().Count(p => p.Id.In(idsToMatch));
                Assert.Equal(expectedCount, queryCount);
            }
        }

        [Fact]
        public void WhereIn_Only_AfterLast()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/6" },
                    0);
            }
        }

        [Fact]
        public void WhereIn_Hit1_Then_AfterLast()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/1", "documents/6" },
                    1);
            }
        }

        [Fact]
        public void WhereIn_Miss2_Then_AfterLast()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/2", "documents/6" },
                    0);
            }
        }

        [Fact]
        public void WhereIn_Hit3_Then_AfterLast()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/3", "documents/6" },
                    1);
            }
        }

        [Fact]
        public void WhereIn_Miss4_Then_AfterLast()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/4", "documents/6" },
                    0);
            }
        }

        [Fact]
        public void WhereIn_Hit5_Then_AfterLast()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/5", "documents/6" },
                    1);
            }
        }



        [Fact]
        public void WhereIn_AfterLast_Then_Hit1()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/6", "documents/1" },
                    1);
            }
        }

        [Fact]
        public void WhereIn_AfterLast_Then_Miss2()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/6", "documents/2" },
                    0);
            }
        }


        [Fact]
        public void WhereIn_AfterLast_Then_Hit3()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/6", "documents/3" },
                    1);
            }
        }


        [Fact]
        public void WhereIn_AfterLast_Then_Miss4()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/6", "documents/4" },
                    0);
            }
        }

        [Fact]
        public void WhereIn_AfterLast_Then_Hit5()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/6", "documents/5" },
                    1);
            }
        }

        [Fact]
        public void WhereIn_Miss2_Then_Miss4()
        {
            using (var store = GetDocumentStore())
            {
                SetupData(store);
                AssertMatches(store,
                    new[] { "documents/2", "documents/4" },
                    0);
            }
        }

    }
}
