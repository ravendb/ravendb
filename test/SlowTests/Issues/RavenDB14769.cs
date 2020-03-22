using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB14769 : RavenTestBase
    {
        public class TestDocument
        {
            public string Name;
            public int Number;
        }
        public RavenDB14769(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDocument { Name = "Hello world! test ", Number = 1 });
                    session.Store(new TestDocument { Name = "Hello test", Number = 2 });
                    session.Store(new TestDocument { Name = "Empty", Number = 3 });
                    session.SaveChanges();
                }

                // WaitForUserToContinueTheTest(store);

                using (var session = store.OpenAsyncSession())
                {
                    // from 'TestDocuments' where Name != null and (exists(Name) and not search(Name, "hello") and not search(Name, "test"))
                    // work correctly
                    var q1 = session.Advanced.AsyncDocumentQuery<TestDocument>()
                        .WhereNotEquals(x => x.Name, (object)null)
                        .OpenSubclause()
                        .Not
                        .Search(e => e.Name, "hello")
                        .AndAlso()
                        .Not
                        .Search(e => e.Name, "test")
                        .CloseSubclause();

                    var result1 = await q1.ToListAsync();
                    Assert.Equal(1, result1.Count);

                    // from 'TestDocuments' where Name != null and (true and not (search(Name, "hello")) and not (search(Name, "test")))
                    // adding search as subclause results returned are wrong
                    var q2 = session.Advanced.AsyncDocumentQuery<TestDocument>()
                        .WhereNotEquals(x => x.Name, (object)null)
                        .OpenSubclause()
                        .Not
                        .OpenSubclause().Search(e => e.Name, "hello").CloseSubclause()
                        .AndAlso()
                        .Not
                        .OpenSubclause().Search(e => e.Name, "test").CloseSubclause()
                        .CloseSubclause();

                    var result2 = await q2.ToListAsync();
                    Assert.Equal(1, result2.Count);

                    // from 'TestDocuments' where Name != null and not (search(Name, "hello")) and not (search(Name, "test"))
                    // adding search as subclause results returned are wrong
                    var q3 = session.Advanced.AsyncDocumentQuery<TestDocument>()
                        .WhereNotEquals(x => x.Name, (object)null)
                        .Not
                        .OpenSubclause().Search(e => e.Name, "hello").CloseSubclause()
                        .AndAlso()
                        .Not
                        .OpenSubclause().Search(e => e.Name, "test").CloseSubclause();

                    var result3 = await q3.ToListAsync();
                    Assert.Equal(1, result3.Count);
                }
            }
        }
    }
}
