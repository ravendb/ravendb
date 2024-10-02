using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Extensions;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_16050 : RavenTestBase
{
    public RavenDB_16050(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public async Task Can_Export_Query_To_Json_File()
    {
        using (var store = GetDocumentStore())
        {
            await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Indexes));

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Company = "companies/1",
                    Employee = "employees/1",
                    Lines =
                    [
                        new() { Discount = 0.1m, PricePerUnit = 70, Product = "products/1", Quantity = 10 }
                    ]
                }, "orders/1");

                await session.StoreAsync(new Order
                {
                    Company = "companies/2",
                    Employee = "employees/2",
                    Lines =
                    [
                        new() { Discount = 0.2m, PricePerUnit = 50, Product = "products/2", Quantity = 100 }
                    ]
                }, "orders/2");

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store);

            // Map
            using var client = new HttpClient().WithConventions(store.Conventions);
            await using (var stream = await client.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Orders/Totals\'&format=json"))
            using (var sr = new StreamReader(stream))
            {
                var array = JArray.Parse(await sr.ReadToEndAsync());
                Assert.Equal(2, array.Count);
                var order1 = array[0].Value<JObject>();
                Assert.Equal("companies/1", order1[nameof(Order.Company)]?.Value<string>());
                Assert.Equal("employees/1", order1[nameof(Order.Employee)]?.Value<string>());
                var order2 = array[1].Value<JObject>();
                Assert.Equal("companies/2", order2[nameof(Order.Company)]?.Value<string>());
                Assert.Equal("employees/2", order2[nameof(Order.Employee)]?.Value<string>());
            }

            await using (var stream = await client.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Orders/Totals\'&format=json&field=@id&field=Company"))
            using (var sr = new StreamReader(stream))
            {
                var array = JArray.Parse(await sr.ReadToEndAsync());
                Assert.Equal(2, array.Count);
                var order1 = array[0].Value<JObject>();
                Assert.Equal("orders/1", order1[Constants.Documents.Metadata.Id]?.Value<string>());
                Assert.Equal("companies/1", order1[nameof(Order.Company)]?.Value<string>());
                Assert.Null(order1[nameof(Order.Employee)]);
                var order2 = array[1].Value<JObject>();
                Assert.Equal("orders/2", order2[Constants.Documents.Metadata.Id]?.Value<string>());
                Assert.Equal("companies/2", order2[nameof(Order.Company)]?.Value<string>());
                Assert.Null(order2[nameof(Order.Employee)]);
            }

            // Map-Reduce
            await using (var stream = await client.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Orders/ByCompany\'&format=json"))
            using (var sr = new StreamReader(stream))
            {
                var array = JArray.Parse(await sr.ReadToEndAsync());
                Assert.Equal(2, array.Count);
                var order1 = array[0].Value<JObject>();
                Assert.Equal("companies/1", order1["Company"]?.Value<string>());
                Assert.Equal(1, order1["Count"]?.Value<int>());
                Assert.Equal(630, order1["Total"]?.Value<decimal>());
                var order2 = array[1].Value<JObject>();
                Assert.Equal("companies/2", order2["Company"]?.Value<string>());
                Assert.Equal(1, order2["Count"]?.Value<int>());
                Assert.Equal(4000, order2["Total"]?.Value<decimal>());
            }

            await using (var stream = await client.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from index \'Orders/ByCompany\'&format=json&field=Count&field=Company"))
            using (var sr = new StreamReader(stream))
            {
                var array = JArray.Parse(await sr.ReadToEndAsync());
                Assert.Equal(2, array.Count);
                var order1 = array[0].Value<JObject>();
                Assert.Equal("companies/1", order1["Company"]?.Value<string>());
                Assert.Equal(1, order1["Count"]?.Value<int>());
                Assert.Null(order1["Total"]);
                var order2 = array[1].Value<JObject>();
                Assert.Equal("companies/2", order2["Company"]?.Value<string>());
                Assert.Equal(1, order2["Count"]?.Value<int>());
                Assert.Null(order2["Total"]);
            }
        }
    }
}
