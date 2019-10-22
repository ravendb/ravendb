using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Lazy
{
    public class MultiGetOperations : RavenTestBase
    {
        public MultiGetOperations(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void UnlessAccessedLazyLoadsAreNoOp()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.Lazily.Load<User>("users/1");
                    session.Advanced.Lazily.Load<User>("users/2");
                    Assert.Equal(0, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void WithPaging()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var u1 = session.Advanced.Lazily.Load<User>(new[] { "users/1" });
                    var u2 = session.Advanced.Lazily.Load<User>(new[] { "users/2" });

                    Assert.Null(u1.Value.Values.FirstOrDefault());
                    Assert.Null(u2.Value.Values.FirstOrDefault());

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
