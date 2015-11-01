using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class Kwoodard : RavenTest
    {
        [Fact]
        public void CanSetUseOptimisticConcurrencyGlobally()
        {
            using (var store = NewDocumentStore())
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
