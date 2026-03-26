using FastTests;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB1019 : RavenTestBase
    {
        public RavenDB1019(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void StreamDocsShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "Test" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream<object>(startsWith:"");

                    var count = 0;
                    while (enumerator.MoveNext())
                    {
                        count++;
                    }

                    Assert.Equal(2, count);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanDisposeEarly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        session.Store(new User() { Name = "Test" });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream<object>(startsWith:"");

                    if (enumerator.MoveNext())
                        enumerator.Dispose();
                }
            }
        }
    }
}