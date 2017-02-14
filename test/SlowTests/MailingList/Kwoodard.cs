using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Kwoodard : RavenNewTestBase
    {
        [Fact]
        public void CanSetUseOptimisticConcurrencyGlobally()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.UseOptimisticConcurrency = true;

                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.UseOptimisticConcurrency);
                }
            }
        }
    }
}
