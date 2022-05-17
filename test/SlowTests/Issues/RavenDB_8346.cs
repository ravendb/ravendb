using Tests.Infrastructure;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Queries;
using Raven.Client.Extensions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8346 : RavenTestBase
    {
        public RavenDB_8346(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void IdShouldBeIncludedWithBothProjections(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                Indexes.WaitForIndexing(store);

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
limit 1"
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
limit 1"
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
