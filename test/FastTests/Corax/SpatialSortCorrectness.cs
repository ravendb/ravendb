using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class SpatialSortCorrectness(ITestOutputHelper output) : RavenTestBase(output)
{
    private const double CenterLat = 52.09;
    private const double CenterLng = 5.12;
    private const double EarthRadiusKm = 6371.0087714;
    private static readonly string CenterPoint = FormattableString.Invariant($"spatial.point({CenterLat}, {CenterLng})");

    private static double DistanceKm(double lat, double lng)
    {
        var dLat = (lat - CenterLat) * Math.PI / 180;
        var dLng = (lng - CenterLng) * Math.PI / 180;
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(CenterLat * Math.PI / 180) * Math.Cos(lat * Math.PI / 180) * Math.Sin(dLng / 2) * Math.Sin(dLng / 2);
        return 2 * EarthRadiusKm * Math.Asin(Math.Sqrt(a));
    }

    private DocumentStore CreateStore(bool includeDistances)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord = record =>
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeSpatialDistance)] = includeDistances.ToString();
        var store = GetDocumentStore(options);
        new PlacesIndex().Execute(store);
        return store;
    }

    private static List<Place> RandomPlaces(int count, int seed)
    {
        var random = new Random(seed);
        return Enumerable.Range(0, count)
            .Select(i => new Place { Id = $"places/{i}", Latitude = 50 + random.NextDouble() * 5, Longitude = 3 + random.NextDouble() * 5 })
            .ToList();
    }

    private void Store(DocumentStore store, IEnumerable<Place> places)
    {
        using (var bulk = store.BulkInsert())
        {
            foreach (var place in places)
                bulk.Store(place, place.Id);
        }

        Indexes.WaitForIndexing(store);
    }

    // entries as close as the geohash precision may swap places, so the order is verified by distance
    private static void AssertSameDistances(IEnumerable<Place> expected, List<Place> actual)
    {
        var expectedDistances = expected.Select(p => DistanceKm(p.Latitude.Value, p.Longitude.Value)).ToList();
        Assert.Equal(expectedDistances.Count, actual.Count);
        for (var i = 0; i < actual.Count; i++)
            Assert.Equal(expectedDistances[i], DistanceKm(actual[i].Latitude.Value, actual[i].Longitude.Value), 0.02);
        Assert.Equal(actual.Count, actual.Select(p => p.Id).Distinct().Count());
    }

    private static string Query(bool descending, int skip, int take) =>
        $"from index 'PlacesIndex' order by spatial.distance(Coordinates, {CenterPoint}){(descending ? " desc" : "")} limit {skip}, {take}";

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void ResultsAreOrderedByTheTrueDistanceAndTakeKeepsTheNearest(bool descending, bool includeDistances)
    {
        using var store = CreateStore(includeDistances);
        var places = RandomPlaces(2_000, 7);
        Store(store, places);

        var expected = places.OrderBy(p => DistanceKm(p.Latitude.Value, p.Longitude.Value)).ToList();
        if (descending)
            expected.Reverse();

        foreach (var take in new[] { 1, 2, 3, 20, 101, 1_999, 2_000, 5_000 })
        {
            using var session = store.OpenSession();
            var results = session.Advanced.RawQuery<Place>(Query(descending, 0, take)).ToList();

            Assert.Equal(Math.Min(take, places.Count), results.Count);
            AssertSameDistances(expected.Take(take), results);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [InlineData(false)]
    [InlineData(true)]
    public void SkipAndTakeReturnTheRightWindow(bool descending)
    {
        using var store = CreateStore(includeDistances: false);
        var places = RandomPlaces(500, 11);
        Store(store, places);

        var expected = places.OrderBy(p => DistanceKm(p.Latitude.Value, p.Longitude.Value)).ToList();
        if (descending)
            expected.Reverse();

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(Query(descending, 30, 25)).ToList();
        AssertSameDistances(expected.Skip(30).Take(25), results);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [InlineData(false)]
    [InlineData(true)]
    public void ReportedDistancesMatchTheTrueDistanceAndAreSorted(bool descending)
    {
        using var store = CreateStore(includeDistances: true);
        var places = RandomPlaces(1_000, 3);
        Store(store, places);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(Query(descending, 0, 40)).ToList();
        Assert.Equal(40, results.Count);

        var distances = new List<double>();
        foreach (var place in results)
        {
            var spatial = (IDictionary<string, object>)session.Advanced.GetMetadataFor(place)["@spatial"];
            var distance = Convert.ToDouble(spatial["Distance"]);
            distances.Add(distance);
            Assert.Equal(DistanceKm(place.Latitude.Value, place.Longitude.Value), distance, 0.05);
        }

        var sorted = descending ? distances.OrderByDescending(x => x) : distances.OrderBy(x => x);
        Assert.Equal(sorted.ToList(), distances);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [InlineData(false)]
    [InlineData(true)]
    public void ProjectionOfTheSpatialDistanceIsTheTrueDistance(bool descending)
    {
        using var store = CreateStore(includeDistances: true);
        var places = RandomPlaces(1_000, 5);
        Store(store, places);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Projection>(
                $"from index 'PlacesIndex' as p order by spatial.distance(Coordinates, {CenterPoint}){(descending ? " desc" : "")} " +
                "select { Id: id(p), Distance: getMetadata(p)['@spatial'].Distance } limit 0, 15")
            .ToList();

        Assert.Equal(15, results.Count);
        var byId = places.ToDictionary(p => p.Id);
        foreach (var result in results)
        {
            var place = byId[result.Id];
            Assert.Equal(DistanceKm(place.Latitude.Value, place.Longitude.Value), result.Distance, 0.05);
        }

        var sorted = descending ? results.OrderByDescending(x => x.Distance) : results.OrderBy(x => x.Distance);
        Assert.Equal(sorted.Select(x => x.Id).ToList(), results.Select(x => x.Id).ToList());
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [InlineData(false)]
    [InlineData(true)]
    public void TiesAreAllAtTheSameDistanceWhenTakeCutsThroughThem(bool includeDistances)
    {
        using var store = CreateStore(includeDistances);
        var places = new List<Place>();
        // 5 at the center, 6 at the very same spot, and a few farther away
        for (var i = 0; i < 5; i++)
            places.Add(new Place { Id = $"center/{i}", Latitude = CenterLat, Longitude = CenterLng });
        for (var i = 0; i < 6; i++)
            places.Add(new Place { Id = $"near/{i}", Latitude = CenterLat + 0.1, Longitude = CenterLng });
        for (var i = 0; i < 10; i++)
            places.Add(new Place { Id = $"far/{i}", Latitude = CenterLat + 1 + i * 0.1, Longitude = CenterLng });
        Store(store, places);

        using (var session = store.OpenSession())
        {
            var three = session.Advanced.RawQuery<Place>(Query(false, 0, 3)).ToList();
            Assert.Equal(3, three.Count);
            Assert.All(three, p => Assert.StartsWith("center/", p.Id));
            Assert.Equal(3, three.Select(p => p.Id).Distinct().Count());
        }

        using (var session = store.OpenSession())
        {
            // cuts through the group: the 5 centers first, then any 2 of the 6
            var seven = session.Advanced.RawQuery<Place>(Query(false, 0, 7)).ToList();
            Assert.Equal(7, seven.Count);
            Assert.All(seven.Take(5), p => Assert.StartsWith("center/", p.Id));
            Assert.All(seven.Skip(5), p => Assert.StartsWith("near/", p.Id));
            Assert.Equal(7, seven.Select(p => p.Id).Distinct().Count());
        }

        using (var session = store.OpenSession())
        {
            var farthest = session.Advanced.RawQuery<Place>(Query(true, 0, 2)).ToList();
            Assert.Equal(new[] { "far/9", "far/8" }, farthest.Select(p => p.Id).ToArray());
        }

        using (var session = store.OpenSession())
        {
            var all = session.Advanced.RawQuery<Place>(Query(false, 0, 100)).ToList();
            Assert.Equal(places.Count, all.Count);
            Assert.Equal(places.Count, all.Select(p => p.Id).Distinct().Count());
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [InlineData(false)]
    [InlineData(true)]
    public void DocumentsWithoutCoordinatesAreReturnedAndDoNotBreakTheOrderOfTheOthers(bool includeDistances)
    {
        using var store = CreateStore(includeDistances);
        var places = RandomPlaces(300, 13);
        var withoutCoordinates = Enumerable.Range(0, 20).Select(i => new Place { Id = $"empty/{i}" }).ToList();
        Store(store, places.Concat(withoutCoordinates));

        var expected = places.OrderBy(p => DistanceKm(p.Latitude.Value, p.Longitude.Value)).ToList();

        foreach (var descending in new[] { false, true })
        {
            using var session = store.OpenSession();
            var all = session.Advanced.RawQuery<Place>(Query(descending, 0, 1_000)).ToList();
            Assert.Equal(places.Count + withoutCoordinates.Count, all.Count);

            var withCoordinates = all.Where(p => p.Latitude != null).ToList();
            AssertSameDistances(descending ? Enumerable.Reverse(expected).ToList() : expected, withCoordinates);

            // the documents without coordinates are grouped together, at the start or at the end
            var positions = all.Select((p, i) => (p, i)).Where(x => x.p.Latitude == null).Select(x => x.i).ToList();
            Assert.Equal(positions.Count, positions[^1] - positions[0] + 1);
            Assert.True(positions[0] == 0 || positions[^1] == all.Count - 1);
        }

        // a small take must still be made of the nearest documents that have coordinates
        using (var session = store.OpenSession())
        {
            var nearest = session.Advanced.RawQuery<Place>(Query(false, 0, 10)).ToList();
            AssertSameDistances(expected.Take(10), nearest);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    public void SpatialSortCombinedWithAFilterKeepsTheOrder()
    {
        using var store = CreateStore(includeDistances: false);
        var places = RandomPlaces(1_000, 21);
        Store(store, places);

        var expected = places.Where(p => p.Latitude > 52)
            .OrderBy(p => DistanceKm(p.Latitude.Value, p.Longitude.Value)).Take(12).ToList();

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Place>(
                $"from index 'PlacesIndex' where Latitude > 52.0 order by spatial.distance(Coordinates, {CenterPoint}) limit 0, 12")
            .ToList();
        AssertSameDistances(expected, results);
    }

    private class Projection
    {
        public string Id { get; set; }
        public double Distance { get; set; }
    }

    private class Place
    {
        public string Id { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
    }

    private class PlacesIndex : AbstractIndexCreationTask<Place>
    {
        public PlacesIndex()
        {
            Map = places => from p in places
                select new
                {
                    p.Latitude,
                    Coordinates = p.Latitude == null || p.Longitude == null ? null : CreateSpatialField(p.Latitude.Value, p.Longitude.Value)
                };
        }
    }
}
