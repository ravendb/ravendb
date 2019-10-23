using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Kwoodard : RavenTestBase
    {
        public Kwoodard(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSetUseOptimisticConcurrencyGlobally()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.UseOptimisticConcurrency = true;
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    Assert.True(session.Advanced.UseOptimisticConcurrency);
                }
            }
        }
    }
}
