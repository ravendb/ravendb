using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Jorre : RavenTestBase
    {
        public Jorre(ITestOutputHelper output) : base(output)
        {
        }

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
