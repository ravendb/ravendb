using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20421 : RavenTestBase
{
    public RavenDB_20421(ITestOutputHelper output) : base(output)
    {
        
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void TestAutoSpatialIndexAfterDatabaseRestart()
    {
        var path = NewDataPath();
        using var store = GetDocumentStore(new Options()
        {
            RunInMemory = false,
            Path = path
        });
        
        using (var session = store.OpenSession())
        {
            var e1 = new Employee() { Name = "Name1", Latitude = 47.623473, Longitude = -122.306009 };
            var e2 = new Employee() { Name = "Name2", Latitude = 0.0, Longitude = 0.0 };

            session.Store(e1);
            session.Store(e2);

            session.SaveChanges();

            var query = session.Query<Employee>().Customize(x => x.WaitForNonStaleResults())
                .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude),
                    criteria => criteria.WithinRadius(1000, 47.56, -122.31));
            
            var res = query.ToList();

            Assert.Equal(1, res.Count);
        }
        
        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
        
        using (var session = store.OpenSession())
        {
            var query = session.Advanced.RawQuery<Employee>(
                @"from index 'Auto/Employees/BySpatial.point(Latitude|Longitude)' where spatial.within('spatial.point(Latitude, Longitude)', spatial.circle(1000, 47.56,-122.31))").WaitForNonStaleResults();

            var e3 = new Employee() { Name = "Name3", Latitude = 47.561, Longitude = -122.311 };

            session.Store(e3);

            session.SaveChanges();

            var resAfterRestart = query.ToList();
            
            Assert.Equal(2, resAfterRestart.Count);
        }
    }

    private class Employee
    {
        public string Name { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }
}
