using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14006 : RavenTestBase
    {
        public RavenDB_14006(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CompareExchangeValueTrackingInSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var address = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(company.ExternalId, address);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.Equal(address, value1.Value);
                    Assert.Equal(company.ExternalId, value1.Key);
                    Assert.Equal(0, value1.Index);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(address, value1.Value);
                    Assert.Equal(company.ExternalId, value1.Key);
                    Assert.True(value1.Index > 0);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(value1, value2);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    session.Advanced.Clear();

                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);
                    Assert.NotEqual(value2, value3);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var address = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/cf");

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");

                    Assert.Equal(numberOfRequests + 2, session.Advanced.NumberOfRequests);

                    var values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { "companies/cf", "companies/hr" });

                    Assert.Equal(numberOfRequests + 2, session.Advanced.NumberOfRequests);

                    Assert.Equal(2, values.Count);
                    Assert.Equal(value1, values[value1.Key]);
                    Assert.Equal(value2, values[value2.Key]);

                    values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { "companies/cf", "companies/hr", "companies/hx" });

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, values.Count);
                    Assert.Equal(value1, values[value1.Key]);
                    Assert.Equal(value2, values[value2.Key]);

                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hx");

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    Assert.Null(value3);
                    Assert.Null(values["companies/hx"]);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    var address = new Address { City = "Bydgoszcz" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hx", address);

                    session.SaveChanges();

                    Assert.Equal(numberOfRequests + 4, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CompareExchangeValueTrackingInSession_NoTracking()
        {
            using (var store = GetDocumentStore())
            {
                var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(company);

                    var address = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(company.ExternalId, address);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, NoTracking = true }))
                {
                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 2, session.Advanced.NumberOfRequests);

                    Assert.NotEqual(value1, value2);

                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    Assert.NotEqual(value2, value3);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, NoTracking = true }))
                {
                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 2, session.Advanced.NumberOfRequests);

                    Assert.NotEqual(value1[company.ExternalId], value2[company.ExternalId]);

                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(company.ExternalId);

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    Assert.NotEqual(value2[company.ExternalId], value3[company.ExternalId]);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide, NoTracking = true }))
                {
                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { company.ExternalId });

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { company.ExternalId });

                    Assert.Equal(numberOfRequests + 2, session.Advanced.NumberOfRequests);

                    Assert.NotEqual(value1[company.ExternalId], value2[company.ExternalId]);

                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { company.ExternalId });

                    Assert.Equal(numberOfRequests + 3, session.Advanced.NumberOfRequests);

                    Assert.NotEqual(value2[company.ExternalId], value3[company.ExternalId]);
                }
            }
        }

        [Fact]
        public void CanUseCompareExchangeValueIncludesInLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    session.Store(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company1 = session.Load<Company>("companies/1", includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(company1.ExternalId);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.NotNull(value1);
                    Assert.True(value1.Index > 0);
                    Assert.Equal(company1.ExternalId, value1.Key);
                    Assert.NotNull(value1.Value);
                    Assert.Equal("Torun", value1.Value.City);

                    var company2 = session.Load<Company>("companies/1", includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.Equal(company1, company2);

                    var employee1 = session.Load<Employee>("employees/1", includes => includes.IncludeCompareExchangeValue(x => x.Notes));

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(employee1.Notes[0]);
                    var value3 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(employee1.Notes[1]);

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(value1, value2);
                    Assert.NotEqual(value2, value3);

                    var values = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(employee1.Notes.ToArray());

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(2, values.Count);
                    Assert.Equal(value2, values[value2.Key]);
                    Assert.Equal(value3, values[value3.Key]);
                }
            }
        }

        [Fact]
        public async Task CanUseCompareExchangeValueIncludesInLoad_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    await session.StoreAsync(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    await session.StoreAsync(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company1 = await session.LoadAsync<Company>("companies/1", includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>(company1.ExternalId);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.NotNull(value1);
                    Assert.True(value1.Index > 0);
                    Assert.Equal(company1.ExternalId, value1.Key);
                    Assert.NotNull(value1.Value);
                    Assert.Equal("Torun", value1.Value.City);

                    var company2 = await session.LoadAsync<Company>("companies/1", includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    Assert.Equal(company1, company2);

                    var employee1 = await session.LoadAsync<Employee>("employees/1", includes => includes.IncludeCompareExchangeValue(x => x.Notes));

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    var value2 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>(employee1.Notes[0]);
                    var value3 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>(employee1.Notes[1]);

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(value1, value2);
                    Assert.NotEqual(value2, value3);

                    var values = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<Address>(employee1.Notes.ToArray());

                    Assert.Equal(numberOfRequests + 1, session.Advanced.NumberOfRequests);

                    Assert.Equal(2, values.Count);
                    Assert.Equal(value2, values[value2.Key]);
                    Assert.Equal(value3, values[value3.Key]);
                }
            }
        }

        [Fact]
        public void CanUseCompareExchangeValueIncludesInQueries_Dynamic()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    session.Store(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var companies = session.Query<Company>()
                        .Statistics(out var stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0);
                    var resultEtag = stats.ResultEtag;

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Torun", value1.Value.City);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    companies = session.Query<Company>()
                        .Statistics(out stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal(-1, stats.DurationInMs); // from cache
                    Assert.Equal(resultEtag, stats.ResultEtag);

                    using (var innerSession = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var value = innerSession.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                        value.Value.City = "Bydgoszcz";

                        innerSession.SaveChanges();
                    }

                    companies = session.Query<Company>()
                        .Statistics(out stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(resultEtag, stats.ResultEtag);

                    value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Bydgoszcz", value1.Value.City);
                }
            }
        }

        [Fact]
        public async Task CanUseCompareExchangeValueIncludesInQueries_Dynamic_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    session.Store(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var companies = session.Advanced
                        .RawQuery<Company>(
                        @"
declare function incl(c) {
    includes.cmpxchg(c.ExternalId);
    return c;
}
from Companies as c
select incl(c)"
                        )
                        .Statistics(out var stats)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0);
                    var resultEtag = stats.ResultEtag;

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Torun", value1.Value.City);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    companies = companies = session.Advanced
                        .RawQuery<Company>(
                        @"
declare function incl(c) {
    includes.cmpxchg(c.ExternalId);
    return c;
}
from Companies as c
select incl(c)"
                        )
                        .Statistics(out stats)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal(-1, stats.DurationInMs); // from cache
                    Assert.Equal(resultEtag, stats.ResultEtag);

                    long lastClusterTxIndex;
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                    using (var innerSession = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var value = innerSession.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                        value.Value.City = "Bydgoszcz";

                        innerSession.SaveChanges();
                        lastClusterTxIndex = value.Index;
                    }

                    await database.RachisLogIndexNotifications.WaitForIndexNotification(lastClusterTxIndex, TimeSpan.FromSeconds(5));

                    companies = companies = session.Advanced
                        .RawQuery<Company>(
                        @"
declare function incl(c) {
    includes.cmpxchg(c.ExternalId);
    return c;
}
from Companies as c
select incl(c)"
                        )
                        .Statistics(out stats)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(resultEtag, stats.ResultEtag);

                    value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Bydgoszcz", value1.Value.City);
                }
            }
        }

        [Fact]
        public void CanUseCompareExchangeValueIncludesInQueries_Static()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    session.Store(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var companies = session.Query<Company, Companies_ByName>()
                        .Statistics(out var stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0);
                    var resultEtag = stats.ResultEtag;

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Torun", value1.Value.City);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    companies = session.Query<Company, Companies_ByName>()
                        .Statistics(out stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal(-1, stats.DurationInMs); // from cache
                    Assert.Equal(resultEtag, stats.ResultEtag);

                    using (var innerSession = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var value = innerSession.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                        value.Value.City = "Bydgoszcz";

                        innerSession.SaveChanges();

                        Indexes.WaitForIndexing(store);
                    }

                    companies = session.Query<Company, Companies_ByName>()
                        .Statistics(out stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(resultEtag, stats.ResultEtag);

                    value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Bydgoszcz", value1.Value.City);
                }
            }
        }

        [Fact]
        public async Task CanUseCompareExchangeValueIncludesInQueries_Static_Async()
        {
            using (var store = GetDocumentStore())
            {
                await new Companies_ByName().ExecuteAsync(store);

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    await session.StoreAsync(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    await session.StoreAsync(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var companies = await session.Query<Company, Companies_ByName>()
                        .Statistics(out var stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToListAsync();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0);
                    var resultEtag = stats.ResultEtag;

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>(companies[0].ExternalId);
                    Assert.Equal("Torun", value1.Value.City);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    companies = await session.Query<Company, Companies_ByName>()
                        .Statistics(out stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToListAsync();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal(-1, stats.DurationInMs); // from cache
                    Assert.Equal(resultEtag, stats.ResultEtag);

                    using (var innerSession = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var value = await innerSession.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>(companies[0].ExternalId);
                        value.Value.City = "Bydgoszcz";

                        await innerSession.SaveChangesAsync();

                        Indexes.WaitForIndexing(store);
                    }

                    companies = await session.Query<Company, Companies_ByName>()
                        .Statistics(out stats)
                        .Include(builder => builder.IncludeCompareExchangeValue(x => x.ExternalId))
                        .ToListAsync();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(resultEtag, stats.ResultEtag);

                    value1 = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Address>(companies[0].ExternalId);
                    Assert.Equal("Bydgoszcz", value1.Value.City);
                }
            }
        }

        [Fact]
        public void CanUseCompareExchangeValueIncludesInQueries_Static_JavaScript()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_ByName().Execute(store);

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var employee = new Employee { Id = "employees/1", Notes = new List<string> { "companies/cf", "companies/hr" } };
                    session.Store(employee);

                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    var address1 = new Address { City = "Torun" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", address1);

                    var address2 = new Address { City = "Hadera" };
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", address2);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var companies = session.Advanced
                        .RawQuery<Company>(
    @"
declare function incl(c) {
    includes.cmpxchg(c.ExternalId);
    return c;
}
from index 'Companies/ByName' as c
select incl(c)"
    )
                        .Statistics(out var stats)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0);
                    var resultEtag = stats.ResultEtag;

                    var numberOfRequests = session.Advanced.NumberOfRequests;

                    var value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Torun", value1.Value.City);

                    Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);

                    companies = session.Advanced
                        .RawQuery<Company>(
    @"
declare function incl(c) {
    includes.cmpxchg(c.ExternalId);
    return c;
}
from index 'Companies/ByName' as c
select incl(c)"
    )
                        .Statistics(out stats)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal(-1, stats.DurationInMs); // from cache
                    Assert.Equal(resultEtag, stats.ResultEtag);

                    using (var innerSession = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var value = innerSession.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                        value.Value.City = "Bydgoszcz";

                        innerSession.SaveChanges();

                        Indexes.WaitForIndexing(store);
                    }

                    companies = session.Advanced
                        .RawQuery<Company>(
    @"
declare function incl(c) {
    includes.cmpxchg(c.ExternalId);
    return c;
}
from index 'Companies/ByName' as c
select incl(c)"
    )
                        .Statistics(out stats)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.True(stats.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(resultEtag, stats.ResultEtag);

                    value1 = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>(companies[0].ExternalId);
                    Assert.Equal("Bydgoszcz", value1.Value.City);
                }
            }
        }

        [Fact]
        public void CompareExchangeValueTrackingInSessionStartsWith()
        {
            using (var store = GetDocumentStore())
            {
                var allCompanies = new List<string>();
                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var company = new Company
                        {
                            Id = $"companies/{i}",
                            ExternalId = "companies/hr",
                            Name = "HR"
                        };

                        allCompanies.Add(company.Id);
                        session.Advanced.ClusterTransaction.CreateCompareExchangeValue(company.Id, company);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var results = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Company>("comp");

                    Assert.Equal(10, results.Count);
                    Assert.True(results.All(x => x.Value != null));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    results = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Company>(allCompanies.ToArray());

                    Assert.Equal(10, results.Count);
                    Assert.True(results.All(x => x.Value != null));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    foreach (var companyId in allCompanies)
                    {
                        var result = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Company>(companyId);
                        Assert.NotNull(result.Value);
                        Assert.Equal(1, session.Advanced.NumberOfRequests);
                    }
                }
            }
        }

        [Fact]
        public async Task CompareExchangeValueTrackingInSessionStartsWithAsync()
        {
            using (var store = GetDocumentStore())
            {
                var allCompanies = new List<string>();
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var company = new Company
                        {
                            Id = $"companies/{i}",
                            ExternalId = "companies/hr",
                            Name = "HR"
                        };

                        allCompanies.Add(company.Id);
                        session.Advanced.ClusterTransaction.CreateCompareExchangeValue(company.Id, company);
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var results = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<Company>("comp");

                    Assert.Equal(10, results.Count);
                    Assert.True(results.All(x => x.Value != null));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    results = await session.Advanced.ClusterTransaction.GetCompareExchangeValuesAsync<Company>(allCompanies.ToArray());

                    Assert.Equal(10, results.Count);
                    Assert.True(results.All(x => x.Value != null));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    foreach (var companyId in allCompanies)
                    {
                        var result = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(companyId);
                        Assert.NotNull(result.Value);
                        Assert.Equal(1, session.Advanced.NumberOfRequests);
                    }
                }
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };
            }
        }
    }
}
