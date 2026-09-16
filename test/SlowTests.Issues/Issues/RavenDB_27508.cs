using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27508 : RavenTestBase
{
    public RavenDB_27508(ITestOutputHelper output) : base(output)
    {
    }

    // Tight enough that the document's geohash cell is only partially covered, so the term generator hands the cell
    // over for a per-entry check - the path that maps Spatial4n's answer onto the query relation. Spatial4n answers
    // Within for a point inside a shape and never Intersects, so an intersects query used to reject the entry.
    private const string Shape = "POLYGON((39.9 22.4, 40.1 22.4, 40.1 22.6, 39.9 22.6, 39.9 22.4))";

    [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void IntersectsMustMatchAPointInsideTheShape(Options options)
    {
        using var store = GetDocumentStore(options);

        Store(store, new Place { Name = "inside", Lat = 22.5, Lng = 40.0 });

        // within is the control: the shape does contain the point, so the index and the query shape are sound
        Assert.Equal(1, Query(store, "within").Count);
        Assert.Equal(1, Query(store, "intersects").Count);
    }

    [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void IntersectsMustNotMatchAPointOutsideTheShape(Options options)
    {
        using var store = GetDocumentStore(options);

        Store(store, new Place { Name = "outside", Lat = 60.0, Lng = 10.0 });

        // no disjoint here - Lucene answers it with UnsupportedSpatialOperation, and that arm is untouched anyway
        Assert.Equal(0, Query(store, "within").Count);
        Assert.Equal(0, Query(store, "intersects").Count);
    }

    private void Store(IDocumentStore store, Place place)
    {
        using (var session = store.OpenSession())
        {
            session.Store(place);
            session.SaveChanges();
        }

        new Places_ByCoordinates().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    private static List<Place> Query(IDocumentStore store, string relation)
    {
        using var session = store.OpenSession();

        return session.Advanced
            .RawQuery<Place>($"from index 'Places/ByCoordinates' as p where spatial.{relation}(p.Coordinates, spatial.wkt('{Shape}'))")
            .ToList();
    }

    private class Place
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public double Lat { get; set; }

        public double Lng { get; set; }
    }

    private class Places_ByCoordinates : AbstractIndexCreationTask<Place>
    {
        public Places_ByCoordinates()
        {
            Map = places => from p in places
                            select new { p.Name, Coordinates = CreateSpatialField(p.Lat, p.Lng) };
        }
    }
}
