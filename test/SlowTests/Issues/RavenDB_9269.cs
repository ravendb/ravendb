using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9269 : RavenTestBase
    {
        public RavenDB_9269(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotThrowNRE()
        {
            using (var store = GetDocumentStore())
            {
                new Itineraries_ByAll().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Query<Itinerary>("Itineraries/ByAll")
                        .Where(i => i.DepartureAirportCode == "AL" && i.ArrivalAirportCode == "PX" && i.TotalDistance >= 1000.00 && i.TotalDistance <= 5000.00)
                        .AggregateBy(i => i.ByField(x => x.DepartureAirportCode).AverageOn(x => x.TotalDistance))
                        .Execute();
                }
            }
        }

        private class Itineraries_ByAll : AbstractIndexCreationTask<Itinerary>
        {
            public Itineraries_ByAll()
            {
                Map = itineraries => from itinerary in itineraries
                                     select new
                                     {
                                         itinerary.ItineraryID,
                                         itinerary.ArrivalAirportCode,
                                         itinerary.DepartureAirportCode,
                                         itinerary.TotalDistance
                                     };
            }
        }

        private class Itinerary
        {
            public string Id { get; set; }
#pragma warning disable 649
            public int ItineraryID; //int
            public string AirlineCode; //varchar
            public string DepartureAirportCode; //char
            public string ArrivalAirportCode; //char
            public double TotalDistance; //real
#pragma warning restore 649
        }
    }
}
