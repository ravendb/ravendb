using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14005 : RavenTestBase
    {
        public RavenDB_14005(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetCompareExchangeValuesLazily()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var lazyValue = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValue<Address>("companies/hr");

                    Assert.False(lazyValue.IsValueCreated);

                    var address = lazyValue.Value;

                    Assert.Null(address);

                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");

                    Assert.Equal(address, value);

                    var lazyValues = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValues<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.False(lazyValues.IsValueCreated);

                    var addresses = lazyValues.Value;

                    Assert.NotNull(addresses);
                    Assert.Equal(2, addresses.Count);
                    Assert.True(addresses.ContainsKey("companies/hr"));
                    Assert.True(addresses.ContainsKey("companies/cf"));
                    Assert.Null(addresses["companies/hr"].Value);
                    Assert.Null(addresses["companies/cf"].Value);

                    var values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.True(values.ContainsKey("companies/hr"));
                    Assert.True(values.ContainsKey("companies/cf"));
                    Assert.Equal(values["companies/hr"].Value, addresses["companies/hr"].Value);
                    Assert.Equal(values["companies/cf"].Value, addresses["companies/cf"].Value);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var lazyValue = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValue<Address>("companies/hr");
                    var lazyValues = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValues<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.False(lazyValue.IsValueCreated);
                    Assert.False(lazyValues.IsValueCreated);

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var address = lazyValue.Value;

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.Null(address);

                    var addresses = lazyValues.Value;

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.NotNull(addresses);
                    Assert.Equal(2, addresses.Count);
                    Assert.True(addresses.ContainsKey("companies/hr"));
                    Assert.True(addresses.ContainsKey("companies/cf"));
                    Assert.Null(addresses["companies/hr"].Value);
                    Assert.Null(addresses["companies/cf"].Value);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var lazyValue = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValueAsync<Address>("companies/hr");

                    Assert.False(lazyValue.IsValueCreated);

                    var address = await lazyValue.Value;

                    Assert.Null(address);

                    var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>("companies/hr");

                    Assert.Equal(address, value);

                    var lazyValues = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValuesAsync<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.False(lazyValues.IsValueCreated);

                    var addresses = await lazyValues.Value;

                    Assert.NotNull(addresses);
                    Assert.Equal(2, addresses.Count);
                    Assert.True(addresses.ContainsKey("companies/hr"));
                    Assert.True(addresses.ContainsKey("companies/cf"));
                    Assert.Null(addresses["companies/hr"].Value);
                    Assert.Null(addresses["companies/cf"].Value);

                    var values = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.True(values.ContainsKey("companies/hr"));
                    Assert.True(values.ContainsKey("companies/cf"));
                    Assert.Equal(values["companies/hr"].Value, addresses["companies/hr"].Value);
                    Assert.Equal(values["companies/cf"].Value, addresses["companies/cf"].Value);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var lazyValue = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValueAsync<Address>("companies/hr");
                    var lazyValues = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValuesAsync<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.False(lazyValue.IsValueCreated);

                    await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var address = await lazyValue.Value;

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.Null(address);

                    var addresses = await lazyValues.Value;

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.NotNull(addresses);
                    Assert.Equal(2, addresses.Count);
                    Assert.True(addresses.ContainsKey("companies/hr"));
                    Assert.True(addresses.ContainsKey("companies/cf"));
                    Assert.Null(addresses["companies/hr"].Value);
                    Assert.Null(addresses["companies/cf"].Value);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Hadera" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var lazyValue = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValue<Address>("companies/hr");

                    Assert.False(lazyValue.IsValueCreated);

                    var address = lazyValue.Value;

                    Assert.NotNull(address);
                    Assert.Equal("Hadera", address.Value.City);

                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");

                    Assert.Equal(address.Value.City, value.Value.City);

                    var lazyValues = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValues<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.False(lazyValues.IsValueCreated);

                    var addresses = lazyValues.Value;

                    Assert.NotNull(addresses);
                    Assert.Equal(2, addresses.Count);
                    Assert.True(addresses.ContainsKey("companies/hr"));
                    Assert.True(addresses.ContainsKey("companies/cf"));
                    Assert.Equal("Hadera", addresses["companies/hr"].Value.City);
                    Assert.Equal("Torun", addresses["companies/cf"].Value.City);

                    var values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.True(values.ContainsKey("companies/hr"));
                    Assert.True(values.ContainsKey("companies/cf"));
                    Assert.Equal(values["companies/hr"].Value.City, addresses["companies/hr"].Value.City);
                    Assert.Equal(values["companies/cf"].Value.City, addresses["companies/cf"].Value.City);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var lazyValue = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValueAsync<Address>("companies/hr");

                    Assert.False(lazyValue.IsValueCreated);

                    var address = await lazyValue.Value;

                    Assert.NotNull(address);
                    Assert.Equal("Hadera", address.Value.City);

                    var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>("companies/hr");

                    Assert.Equal(address.Value.City, value.Value.City);

                    var lazyValues = session.Advanced.ClusterTransaction.Lazily.GetCompareExchangeValuesAsync<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.False(lazyValues.IsValueCreated);

                    var addresses = await lazyValues.Value;

                    Assert.NotNull(addresses);
                    Assert.Equal(2, addresses.Count);
                    Assert.True(addresses.ContainsKey("companies/hr"));
                    Assert.True(addresses.ContainsKey("companies/cf"));
                    Assert.Equal("Hadera", addresses["companies/hr"].Value.City);
                    Assert.Equal("Torun", addresses["companies/cf"].Value.City);

                    var values = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.True(values.ContainsKey("companies/hr"));
                    Assert.True(values.ContainsKey("companies/cf"));
                    Assert.Equal(values["companies/hr"].Value.City, addresses["companies/hr"].Value.City);
                    Assert.Equal(values["companies/cf"].Value.City, addresses["companies/cf"].Value.City);
                }
            }
        }
    }
}
