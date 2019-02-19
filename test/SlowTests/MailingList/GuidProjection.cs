using System;
using System.Linq;
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
        public void CanProjectGuids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView { TestField = Guid.NewGuid() });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var good = session.Query<TestView>().Customize(x => x.WaitForNonStaleResults()).Select(x => new { x.TestField }).ToArray();
                    var error = session.Query<TestView>().Customize(x => x.WaitForNonStaleResults()).Select(x => x.TestField).ToArray();
                    var error2 = session.Query<TestView>().Select(x => (Guid)x.TestField).ToArray();
                }
            }
        }
    }
}
