using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.MailingList.PhilJones
{
    public class Projections : RavenTestBase
    {
        public Projections(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void WorkWithRealTypes(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<Offer>()
                        .Where(x => x.TripId == "trips/1234")
                        .OrderBy(x => x.Name)
                        .Select(x => new SelectListItem
                        {
                            Text = x.Name,
                            Value = x.Id
                        })
                        .ToList();
                }
            }
        }

        private class SelectListItem
        {
            public string Text { get; set; }
            public string Value { get; set; }
        }

        private class Offer
        {
            public string Id { get; set; }
            public string TripId { get; set; }
            public string Name { get; set; }
        }
    }

}
