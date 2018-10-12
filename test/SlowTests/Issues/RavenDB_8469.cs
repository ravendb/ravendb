using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8469 : RavenTestBase
    {
        [Fact]
        public void ProjectionShouldBeIncludedWithBothProjections()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var qr = commands.Query(new IndexQuery
                    {
                        Query = @"declare function upper(o){
  return o.Name.toUpperCase();
}
from Orders as o
where o.ShipTo.City = 'London'
load o.Company as c
select upper(c) as CompanyName, o.Employee
fetch 1"
                    });

                    var json = (BlittableJsonReaderObject)qr.Results[0];
                    var metadata = json.GetMetadata();
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.Projection, out bool projection));
                    Assert.True(projection);

                    qr = commands.Query(new IndexQuery
                    {
                        Query = @"declare function upper(o){
  return o.Name.toUpperCase();
}
from Orders as o
where o.ShipTo.City = 'London'
load o.Company as c
select {
    CompanyName: upper(c),
    Employee: o.Employee
}
fetch 1"
                    });

                    json = (BlittableJsonReaderObject)qr.Results[0];
                    metadata = json.GetMetadata();
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.Projection, out projection));
                    Assert.True(projection);
                }
            }
        }

        [Fact]
        public void ResultsFromProjectionShouldNotBeTrackedBySession()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Advanced
                        .RawQuery<object>(@"declare function upper(o){
  return o.Name.toUpperCase();
}
from Orders as o
where o.ShipTo.City = 'London'
load o.Company as c
select {
    CompanyName: upper(c),
    Employee: o.Employee
}")
                        .ToList();

                    var s = (DocumentSession)session;
                    Assert.Equal(0, s.DocumentsById.Count);
                }
            }
        }
    }
}
