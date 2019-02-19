using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class AnyBugTest : RavenTestBase
    {
        private class AnyBug
        {
            public string Id { get; set; }
            public string[] Bugs { get; set; }
        }

        private class MyIndex : AbstractIndexCreationTask<AnyBug>
        {
            public MyIndex()
            {
                Map = bugs => from anyBug in bugs
                              from bug in anyBug.Bugs
                              select new { Bugs = bug };
            }
        }

        [Fact]
        public void FailTest()
        {
            using (var docStore = GetDocumentStore())
            {
                //Create Data
                CreateAnyOperatorBug(docStore);

                new MyIndex().Execute(docStore);

                //Failing Query
                //This may create temp index : from doc in docs.anybugs select new { Bugs = doc.Bugs }
                using (var session = docStore.OpenSession())
                {
                    var q = session.Query<AnyBug, MyIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Bugs.Any(y => y.StartsWith("roles/1") && y.EndsWith("staffs/2")));

                    int resultCount = q.ToList().Count();

                    Assert.Equal<int>(1, resultCount);
                }

            }
        }

        [Fact]
        public void PassTest()
        {
            using (var store = GetDocumentStore())
            {
                //Create Data
                CreateAnyOperatorBug(store);

                //Good Query if processed by client side LINQ
                using (var session = store.OpenSession())
                {
                    int resultCount = session.Query<AnyBug>()
                    .ToList()

                    .Where(x => x.Bugs.Any(y => y.StartsWith("roles/1") && y.EndsWith("staffs/2")))
                    .Count();

                    Assert.Equal<int>(1, resultCount);
                }
            }
        }

        private void CreateAnyOperatorBug(IDocumentStore docStore)
        {
            using (var session = docStore.OpenSession())
            {
                var bug1 = new AnyBug
                {
                    Id = "anybugs/1",
                    Bugs = new string[]
                    {
                        "roles/1,staffs/2",
                        "roles/2,staffs/1"
                    }
                };

                var bug2 = new AnyBug
                {
                    Id = "anybugs/2",
                    Bugs = new string[]
                    {
                        "roles/1,staffs/1",
                        "roles/2,staffs/2"
                    }
                };

                session.Store(bug1);
                session.Store(bug2);
                session.SaveChanges();
            }

        }
    }

}
