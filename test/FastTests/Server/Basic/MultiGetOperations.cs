using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class MultiGetOperations : RavenTestBase
    {
        public MultiGetOperations(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void UnlessAccessedLazyLoadsAreNoOp(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.Lazily.Load<User>("users/1");
                    session.Advanced.Lazily.Load<User>("users/2");
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void WithPaging(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var u1 = session.Advanced.Lazily.Load<User>(new[] { "users/1" });
                    var u2 = session.Advanced.Lazily.Load<User>(new[] { "users/2" });

                    Assert.Null(u1.Value["users/1"]);
                    Assert.Null(u2.Value["users/2"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
