using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_372 : RavenTestBase
    {
        public RDBC_372(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task RawQueryWithBlittableJsonReturnType_WithGroupByAndSelect()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<BlittableJsonReaderObject>("from Orders group by ShipVia select ShipVia")
                        .ToListAsync();

                    Assert.Equal(3, query.Count);

                    var expectedShippers = new List<string>
                    {
                        "shippers/1-A", "shippers/2-A", "shippers/3-A"
                    };

                    foreach (var blittable in query)
                    {
                        Assert.Equal(2, blittable.Count);
                        Assert.True(blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));

                        Assert.True(blittable.TryGet("ShipVia", out string str));
                        Assert.True(expectedShippers.Remove(str));
                    }

                }
            }

        }

        [Fact]
        public async Task RawQueryWithBlittableJsonReturnType_WithGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<BlittableJsonReaderObject>("from Orders group by ShipVia")
                        .ToListAsync();

                    Assert.Equal(3, query.Count);

                    var expectedShippers = new List<string>
                    {
                        "shippers/1-A", "shippers/2-A", "shippers/3-A"
                    };

                    foreach (var blittable in query)
                    {
                        Assert.Equal(2, blittable.Count);
                        Assert.True(blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));

                        Assert.True(blittable.TryGet("ShipVia", out string str));
                        Assert.True(expectedShippers.Remove(str));
                    }

                }
            }

        }

        [Fact]
        public void RawQueryWithBlittableJsonReturnType()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<BlittableJsonReaderObject>("from Orders")
                        .ToList();

                    Assert.Equal(830, query.Count);
                }
            }

        }


        [Fact]
        public void RawQueryWithBlittableJsonReturnType_SimpleProjection()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<BlittableJsonReaderObject>("from Orders where Company = 'companies/1-A' select ShipVia, Freight")
                        .ToList();

                    Assert.Equal(6, query.Count);

                    foreach (var blittable in query)
                    {
                        Assert.Equal(3, blittable.Count);
                        Assert.True(blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                        Assert.True(blittable.TryGet("ShipVia", out string s));
                        Assert.True(blittable.TryGet("Freight", out double d));
                    }
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void RawQueryWithBlittableJsonReturnType_JsProjection(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced
                        .RawQuery<BlittableJsonReaderObject>(
                            @"from Orders as o 
                            where o.Company = 'companies/1-A'
                            select {
                                Shipper : load(o.ShipVia).Name,
                                ShipTo : o.ShipTo.City
                            }")
                        .ToList();

                    Assert.Equal(6, query.Count);

                    var expectedShippers = new[]
                    {
                        "Speedy Express", "United Package", "Federal Shipping"
                    };

                    foreach (var blittable in query)
                    {
                        Assert.Equal(3, blittable.Count);
                        Assert.True(blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));

                        Assert.True(blittable.TryGet("Shipper", out string shipper));
                        Assert.Contains(shipper, expectedShippers);

                        Assert.True(blittable.TryGet("ShipTo", out string city));
                        Assert.Equal("Berlin", city);
                    }
                }
            }
        }


        [Fact]
        public void StreamRawQueryWithBlittableJsonReturnType()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<BlittableJsonReaderObject>(
                        "from Orders where Company = 'companies/1-A' select ShipVia, Freight");

                    var stream = session.Advanced.Stream(query);

                    int count = 0;

                    while (stream.MoveNext())
                    {
                        var doc = stream.Current?.Document;

                        Assert.NotNull(doc);
                        Assert.Equal(3, doc.Count);
                        Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                        Assert.True(doc.TryGet("ShipVia", out string s));
                        Assert.True(doc.TryGet("Freight", out double d));

                        count++;
                    }

                    Assert.Equal(6, count);

                }
            }
        }
    }
}
