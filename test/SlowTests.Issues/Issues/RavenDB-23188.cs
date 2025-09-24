using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23188 : RavenTestBase
    {
        public RavenDB_23188(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OnSession()
        {
            using (var store = GetDocumentStore())
            {

                var session = store.OpenSession(new SessionOptions()
                {
                    NoCaching = true
                });

                session.Query<Product>().ToList();

                Assert.Equal(0, session.Advanced.RequestExecutor.Cache.NumberOfItems);
            }

            using (var store = GetDocumentStore())
            {

                var session = store.OpenSession(new SessionOptions());

                session.Query<Product>().ToList();

                Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void OnQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentQuery<Product>()
                        .NoCaching().ToList();

                    Assert.Equal(0, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                }


                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentQuery<Product>().ToList();

                    Assert.Equal(1, session.Advanced.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }
    }
}
