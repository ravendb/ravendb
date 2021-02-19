using FastTests;
using Orders;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15997 : RavenTestBase
    {
        public RavenDB_15997(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldHandleAliasesInSpatialAutoIndexesProperly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Advanced.RawQuery<Employee>(@"from Employees
where spatial.within(
    spatial.point(Address.Location.Latitude, Address.Location.Longitude),
    spatial.circle(120, 47.448, -122.309, 'miles')
) or
spatial.within(
    spatial.point(Address2.Location.Latitude, Address2.Location.Longitude),
    spatial.circle(20, 48.448, -121.309, 'miles')
)")
    .ToList();

                    session.Advanced.RawQuery<Employee>(@"from Employees as e
where spatial.within(
    spatial.point(e.Address.Location.Latitude, e.Address.Location.Longitude),
    spatial.circle(120, 47.448, -122.309, 'miles')
) or
spatial.within(
    spatial.point(e.Address2.Location.Latitude, e.Address2.Location.Longitude),
    spatial.circle(20, 48.448, -121.309, 'miles')
)")
                        .ToList();

                    session.Advanced.RawQuery<Employee>(@"from Employees as ee
where spatial.within(
    spatial.point(ee.Address.Location.Latitude, ee.Address.Location.Longitude),
    spatial.circle(120, 47.448, -122.309, 'miles')
)")
                        .ToList();

                    var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));
                    Assert.Equal(1, indexes.Length);

                    var index = indexes[0];
                    Assert.Equal("Auto/Employees/BySpatial.point(Address.Location.Latitude|Address.Location.Longitude)AndSpatial.point(Address2.Location.Latitude|Address2.Location.Longitude)", index.Name);
                }
            }
        }
    }
}
