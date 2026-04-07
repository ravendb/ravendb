using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_26145(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void SpatialAutoIndexCanWorkNestedPoint(Options options)
    {
        using var store = GetStoreWithDocuments(options);
        using var session = store.OpenSession();
        
        var result = session.Advanced
            .RawQuery<Document>(@"from 'Documents' order by spatial.distance(spatial.point(Location.Latitude, Location.Longitude), spatial.point(0, 0))")
            .Statistics(out var stats)
            .WaitForNonStaleResults()
            .ToList();
        Assert.Equal("Auto/Documents/BySpatial.point(Location.Latitude|Location.Longitude)", stats.IndexName);
        
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("close", result[0].Name);
        Assert.Equal("far", result[1].Name);

        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));
        var aliased = session.Advanced
            .RawQuery<Document>(@"from 'Documents' myAlias order by spatial.distance(spatial.point(myAlias.Location.Latitude, myAlias.Location.Longitude), spatial.point(0, 0))")
            .Statistics(out var statsAliased)
            .WaitForNonStaleResults()
            .ToList();
        
        Assert.NotNull(aliased);
        Assert.Equal(2, aliased.Count);
        Assert.Equal("close", aliased[0].Name);
        Assert.Equal("far", aliased[1].Name);
        Assert.Equal(stats.IndexName, statsAliased.IndexName);
    }
    
    [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void SpatialAutoIndexCanWorkNestedWkt(Options options)
    {
        using var store = GetStoreWithDocuments(options);
        using var session = store.OpenSession();
        
        var result = session.Advanced
            .RawQuery<Document>(@"from 'Documents' order by spatial.distance(spatial.wkt(Location.Wkt), spatial.point(0, 0))")
            .Statistics(out var stats)
            .WaitForNonStaleResults()
            .ToList();
        Assert.Equal("Auto/Documents/BySpatial.wkt(Location.Wkt)", stats.IndexName);
        
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("close", result[0].Name);
        Assert.Equal("far", result[1].Name);

        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));
        var aliased = session.Advanced
            .RawQuery<Document>(@"from 'Documents' myAlias order by spatial.distance(spatial.wkt(myAlias.Location.Wkt), spatial.point(0, 0))")
            .Statistics(out var statsAliased)
            .WaitForNonStaleResults()
            .ToList();
        
        Assert.NotNull(aliased);
        Assert.Equal(2, aliased.Count);
        Assert.Equal("close", aliased[0].Name);
        Assert.Equal("far", aliased[1].Name);
        Assert.Equal(stats.IndexName, statsAliased.IndexName);
        
        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));
        aliased = session.Advanced
            .RawQuery<Document>(@"from 'Documents' myAlias order by spatial.distance(spatial.wkt(Location.Wkt), spatial.point(0, 0))")
            .Statistics(out statsAliased)
            .WaitForNonStaleResults()
            .ToList();
        
        Assert.NotNull(aliased);
        Assert.Equal(2, aliased.Count);
        Assert.Equal("close", aliased[0].Name);
        Assert.Equal("far", aliased[1].Name);
        Assert.Equal(stats.IndexName, statsAliased.IndexName);
    }
    
    [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void SpatialAutoIndexCanWorkDoubleNested(Options options)
    {
        using var store = GetStoreWithDocuments(options);
        using var session = store.OpenSession();
        var result = session.Advanced
            .RawQuery<Document>(@"from 'Documents' order by spatial.distance(spatial.point(Address.Location.Latitude, Address.Location.Longitude), spatial.point(0, 0))")
            .Statistics(out var stats)
            .WaitForNonStaleResults()
            .ToList();
        Assert.Equal("Auto/Documents/BySpatial.point(Address.Location.Latitude|Address.Location.Longitude)", stats.IndexName);
        
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("close", result[0].Name);
        Assert.Equal("far", result[1].Name);
        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));

        var aliased = session.Advanced
            .RawQuery<Document>(@"from 'Documents' myAlias order by spatial.distance(spatial.point(myAlias.Address.Location.Latitude, myAlias.Address.Location.Longitude), spatial.point(0, 0))")
            .Statistics(out var statsAliased)
            .WaitForNonStaleResults()
            .ToList();
        
        Assert.NotNull(aliased);
        Assert.Equal(2, aliased.Count);
        Assert.Equal("close", aliased[0].Name);
        Assert.Equal("far", aliased[1].Name);
        Assert.Equal(stats.IndexName, statsAliased.IndexName);
    }

    [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void SpatialPointAutoIndexCanWorkLinq(Options options)
    {
        using var store = GetStoreWithDocuments(options);
        using var session = store.OpenSession();

        var result = session.Query<Document>()
            .Customize(x => x.WaitForNonStaleResults())
            .Statistics(out var stats)
            .OrderByDistance(pnt => pnt.Point(doc => doc.Location.Latitude, doc => doc.Location.Longitude), 0, 0)
            .ToList();
        Assert.Equal("Auto/Documents/BySpatial.point(Location.Latitude|Location.Longitude)", stats.IndexName);
        
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("close", result[0].Name);
        Assert.Equal("far", result[1].Name);
        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));

        result = session.Query<Document>()
            .Customize(x => x.WaitForNonStaleResults())
            .Statistics(out stats)
            .OrderByDistanceDescending(pnt => pnt.Point(doc => doc.Location.Latitude, doc => doc.Location.Longitude), 0, 0)
            .ToList();
        Assert.Equal("Auto/Documents/BySpatial.point(Location.Latitude|Location.Longitude)", stats.IndexName);
        
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("far", result[0].Name);
        Assert.Equal("close", result[1].Name);
    }
    
    [RavenTheory(RavenTestCategory.Spatial | RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Lucene)]
    public void SpatialWktPointAutoIndexCanWorkLinq(Options options)
    {
        using var store = GetStoreWithDocuments(options);
        using var session = store.OpenSession();

        var result = session.Query<Document>()
            .Customize(x => x.WaitForNonStaleResults())
            .Statistics(out var stats)
            .OrderByDistance(pnt => pnt.Wkt(doc => doc.Location.Wkt), 0, 0)
            .ToList();
        
        Assert.Equal("Auto/Documents/BySpatial.wkt(Location.Wkt)", stats.IndexName);
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("close", result[0].Name);
        Assert.Equal("far", result[1].Name);
        store.Maintenance.Send(new DeleteIndexOperation(stats.IndexName));
        
        result = session.Query<Document>()
            .Customize(x => x.WaitForNonStaleResults())
            .Statistics(out stats)
            .OrderByDistanceDescending(pnt => pnt.Wkt(doc => doc.Location.Wkt), 0, 0)
            .ToList();
        Assert.Equal("Auto/Documents/BySpatial.wkt(Location.Wkt)", stats.IndexName);
        
        Assert.NotNull(result);
        Assert.Equal(2, result.Count);
        Assert.Equal("far", result[0].Name);
        Assert.Equal("close", result[1].Name);
    }
    
    private IDocumentStore GetStoreWithDocuments(Options options)
    {
        IDocumentStore store = null;
        try
        {
            store = GetDocumentStore(options);
            using var session = store.OpenSession();
            session.Store(new Document
            {
                Name = "far",
                Address = new Address("test", new Location(50, 50)),
                Location = new Location(50, 50)
            });

            session.Store(new Document
            {
                Name = "close",
                Address = new Address("test", new Location(10, 10)),
                Location = new Location(10, 10)
            });

            session.SaveChanges();
        }
        catch
        {
            store?.Dispose();
            throw;
        }

        return store;
    }
    
    private class Document
    {
        public string Name { get; set; }
        public Address Address { get; set; }
        public Location Location { get; set; }
    }
    
    private class Location
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        
        public string Wkt { get; set; }

        public Location()
        {
        }
        
        public Location(double latitude, double longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
            Wkt = $"POINT({longitude} {latitude})";
        }
    }

    private record Address(string Street, Location Location);
}
