using System.Linq;
using Raven.Client.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class SpatialQueries
    {
        [Fact]
        public void CanRunSpatialQueriesInMemory()
        {
            var documentStore = new EmbeddableDocumentStore { RunInMemory = true };
            documentStore.Initialize();
            var def = new IndexDefinition<Listing>
            {
                Map = listings => from listingItem in listings
                                  select new
                                  {
                                      listingItem.ClassCodes,
                                      listingItem.Latitude,
                                      listingItem.Longitude,
                                      _ = SpatialIndex.Generate(listingItem.Latitude, listingItem.Longitude)
                                  }
            };
            documentStore.DatabaseCommands.PutIndex("RadiusClassifiedSearch", def);
        }

        public class Listing
        {
            public string ClassCodes { get; set; }
            public long Latitude { get; set; }
            public long Longitude { get; set; }
        }
    }
}