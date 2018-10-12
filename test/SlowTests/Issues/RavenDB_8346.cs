using FastTests;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8346 : RavenTestBase
    {
        [Fact]
        public void IdShouldBeIncludedWithBothProjections()
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
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.Id, out string id));
                    Assert.False(string.IsNullOrEmpty(id));

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
                    Assert.True(metadata.TryGet(Constants.Documents.Metadata.Id, out id));
                    Assert.False(string.IsNullOrEmpty(id));
                }
            }
        }
    }
}
