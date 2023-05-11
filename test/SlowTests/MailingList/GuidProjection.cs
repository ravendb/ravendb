using System;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class GuidProjection : RavenTestBase
    {
        public GuidProjection(ITestOutputHelper output) : base(output)
        {
        }

        private class TestView
        {
            public Guid TestField { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanProjectGuids(Options options)
        {
            using (var store = GetDocumentStore(options))
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
