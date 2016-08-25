using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Kwoodard : RavenTestBase
    {
        [Fact]
        public void CanSetUseOptimisticConcurrencyGlobally()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.DefaultUseOptimisticConcurrency = true;

                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.UseOptimisticConcurrency);
                }
            }
        }
    }
}
