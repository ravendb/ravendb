using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16537 : RavenTestBase
    {
        public RavenDB_16537(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Use_OnSessionDisposing_Event()
        {
            using (IDocumentStore store = GetDocumentStore())
            {
                int counter = 0;

                using (var session = store.OpenSession())
                {
                    session.Advanced.OnSessionDisposing += (sender, args) =>
                    {
                        Assert.Same(session, sender);
                        Assert.Same(session, args.Session);

                        counter++;
                    };
                }

                Assert.Equal(1, counter);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.OnSessionDisposing += (sender, args) =>
                    {
                        Assert.Same(session, sender);
                        Assert.Same(session, args.Session);

                        counter++;
                    };
                }

                Assert.Equal(2, counter);

                store.OnSessionDisposing += (sender, args) => counter++;

                using (var session = store.OpenSession())
                {
                }

                Assert.Equal(3, counter);

                using (var session = store.OpenAsyncSession())
                {
                }

                Assert.Equal(4, counter);
            }
        }
    }
}
