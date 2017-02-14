using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Jorre : RavenTestBase
    {
        [Fact]
        public void CanQueryOnNegativeDecimal()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Boat>()
                        .Where(x => x.Weight == -1)
                        .ToList();
                }
            }
        }

        private class Boat
        {
            public decimal Weight { get; set; }
        }
    }
}
