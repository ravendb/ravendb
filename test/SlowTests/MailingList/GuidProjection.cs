using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class GuidProjection : RavenTestBase
    {
        private class TestView
        {
            public Guid TestField { get; set; }
        }

        [Fact]
        public async Task CanProjectGuids()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView { TestField = Guid.NewGuid() });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var good = session.Query<TestView>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Select(x => new { x.TestField }).ToArray();
                    var error = session.Query<TestView>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).Select(x => x.TestField).ToArray();
                    var error2 = session.Query<TestView>().Select(x => (Guid)x.TestField).ToArray();
                }
            }
        }
    }
}
