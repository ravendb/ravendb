using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.MailingList
{
    public class Kwoodard : RavenTestBase
    {
        public Kwoodard(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanSetUseOptimisticConcurrencyGlobally()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    s.Conventions.UseOptimisticConcurrency = true;
#pragma warning restore CS0618 // Type or member is obsolete
                }
            }))
            {
                using (var session = store.OpenSession())
                {
#pragma warning disable CS0618 // Type or member is obsolete
                    Assert.True(session.Advanced.UseOptimisticConcurrency);
#pragma warning restore CS0618 // Type or member is obsolete
                }
            }
        }
    }
}
