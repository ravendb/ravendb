using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Extensions;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing.TimeSeries
{
    public class BasicTimeSeriesIndexes_StrongSyntax : RavenTestBase
    {
        public BasicTimeSeriesIndexes_StrongSyntax(ITestOutputHelper output)
            : base(output)
        {
        }

        private class MyTsIndex : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public MyTsIndex()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.TimeStamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

        private class MyTsIndex_Load : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public MyTsIndex_Load()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let employee = LoadDocument<Employee>(entry.Tag)
                                  select new
                                  {
                                      HeartBeat = entry.Value,
                                      entry.TimeStamp.Date,
                                      User = ts.DocumentId,
                                      Employee = employee.FirstName
                                  });
            }
        }

        private class AverageHeartRateDaily_ByDateAndUser : AbstractTimeSeriesIndexCreationTask<User, AverageHeartRateDaily_ByDateAndUser.Result>
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Date { get; set; }

                public string User { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRateDaily_ByDateAndUser()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new Result
                                  {
                                      HeartBeat = entry.Value,
                                      Date = new DateTime(entry.TimeStamp.Date.Year, entry.TimeStamp.Date.Month, entry.TimeStamp.Date.Day),
                                      User = ts.DocumentId,
                                      Count = 1
                                  });

                Reduce = results => from r in results
                                    group r by new { r.Date, r.User } into g
                                    let sumHeartBeat = g.Sum(x => x.HeartBeat)
                                    let sumCount = g.Sum(x => x.Count)
                                    select new Result
                                    {
                                        HeartBeat = sumHeartBeat / sumCount,
                                        Date = g.Key.Date,
                                        User = g.Key.User,
                                        Count = sumCount
                                    };
            }
        }

        private class AverageHeartRateDaily_ByDateAndCity : AbstractTimeSeriesIndexCreationTask<User, AverageHeartRateDaily_ByDateAndCity.Result>
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Date { get; set; }

                public string City { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRateDaily_ByDateAndCity()
            {
                AddMap("HeartRate", timeSeries => from ts in timeSeries
                                                  from entry in ts.Entries
                                                  let address = LoadDocument<Address>(entry.Tag)
                                                  select new Result
                                                  {
                                                      HeartBeat = entry.Value,
                                                      Date = new DateTime(entry.TimeStamp.Date.Year, entry.TimeStamp.Date.Month, entry.TimeStamp.Date.Day),
                                                      City = address.City,
                                                      Count = 1
                                                  });

                Reduce = results => from r in results
                                    group r by new { r.Date, r.City } into g
                                    let sumHeartBeat = g.Sum(x => x.HeartBeat)
                                    let sumCount = g.Sum(x => x.Count)
                                    select new
                                    {
                                        HeartBeat = sumHeartBeat / sumCount,
                                        g.Key.Date,
                                        g.Key.City,
                                        Count = sumCount
                                    };
            }
        }

        [Fact]
        public void BasicMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company).Append("HeartRate", now1, "tag", new double[] { 7 });

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyTsIndex();
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Companies.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Values[0],\r\n    Date = entry.TimeStamp.Date,\r\n    User = ts.DocumentId\r\n})", indexDefinition.Maps.First());

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company).Append("HeartRate", now2, "tag", new double[] { 3 });

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MyTsIndex")).EntriesCount, 2));

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains(now1.Date.GetDefaultRavenFormat(), terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);

                // delete time series

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company).Remove("HeartRate", now2);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("7", terms);

                // delete document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);


                // delete document - this time don't stop indexing to make sure doc deletion will be noticed by the index

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/2");
                    session.TimeSeriesFor(company).Append("HeartRate", now1, "tag", new double[] { 9 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("9", terms);


                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public async Task BasicMapIndexWithLoad()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var employee = new Employee
                    {
                        FirstName = "John"
                    };
                    session.Store(employee, "employees/1");

                    var company = new Company();
                    session.Store(company, "companies/1");

                    session.TimeSeriesFor(company).Append("HeartRate", now1, employee.Id, new double[] { 7 });

                    var company2 = new Company();
                    session.Store(company2, "companies/11");

                    session.TimeSeriesFor(company2).Append("HeartRate", now1, employee.Id, new double[] { 11 });

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyTsIndex_Load();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Companies.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    ts = ts,\r\n    entry = entry\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    employee = this.LoadDocument(this0.entry.Tag, \"Employees\")\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.entry.Value,\r\n    Date = this1.this0.entry.TimeStamp.Date,\r\n    User = this1.this0.ts.DocumentId,\r\n    Employee = this1.employee.FirstName\r\n})", indexDefinition.Maps.First());

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("john", terms);

                using (var session = store.OpenSession())
                {
                    var employee = session.Load<Employee>("employees/1");
                    employee.FirstName = "Bob";

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("bob", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(0, terms.Length);

                // delete source document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexName);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/11");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                Assert.Equal(0, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 0));

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [Fact]
        public void BasicMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                var today = DateTime.Today;
                var tomorrow = today.AddDays(1);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, "users/1");

                    for (int i = 0; i < 10; i++)
                    {
                        session.TimeSeriesFor(user).Append("HeartRate", today.AddHours(i), "abc", new double[] { 180 + i });
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new AverageHeartRateDaily_ByDateAndUser();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Users.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Value,\r\n    Date = new DateTime((int) entry.TimeStamp.Date.Year, (int) entry.TimeStamp.Date.Month, (int) entry.TimeStamp.Date.Day),\r\n    User = ts.DocumentId,\r\n    Count = 1\r\n})", indexDefinition.Maps.First());
                RavenTestHelper.AssertEqualRespectingNewLines("results.GroupBy(r => new {\r\n    Date = r.Date,\r\n    User = r.User\r\n}).Select(g => new {\r\n    g = g,\r\n    sumHeartBeat = Enumerable.Sum(g, x => ((double) x.HeartBeat))\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    sumCount = Enumerable.Sum(this0.g, x0 => ((long) x0.Count))\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.sumHeartBeat / ((double) this1.sumCount),\r\n    Date = this1.this0.g.Key.Date,\r\n    User = this1.this0.g.Key.User,\r\n    Count = this1.sumCount\r\n})", indexDefinition.Reduce);

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("184.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains(today.Date.GetDefaultRavenFormat(), terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("10", terms[0]);

                store.Maintenance.Send(new StopIndexingOperation());

                // add more heart rates
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    for (int i = 0; i < 20; i++)
                    {
                        session.TimeSeriesFor(user).Append("HeartRate", tomorrow.AddHours(i), "abc", new double[] { 200 + i });
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("184.5", terms);
                Assert.Contains("209.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains(today.Date.GetDefaultRavenFormat(), terms);
                Assert.Contains(tomorrow.Date.GetDefaultRavenFormat(), terms);


                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("10", terms);
                Assert.Contains("20", terms);


                store.Maintenance.Send(new StopIndexingOperation());

                //// delete some time series

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    for (int i = 0; i < 10; i++)
                    {
                        session.TimeSeriesFor(user).Remove("HeartRate", today.AddHours(i));
                        session.TimeSeriesFor(user).Remove("HeartRate", tomorrow.AddHours(i));
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("214.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains(tomorrow.Date.GetDefaultRavenFormat(), terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("10", terms[0]);

                //// delete document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                WaitForUserToContinueTheTest(store);
            }
        }

        [Fact]
        public async Task BasicMapReduceIndexWithLoad()
        {
            {
                using (var store = GetDocumentStore())
                {
                    var today = DateTime.Today;

                    using (var session = store.OpenSession())
                    {
                        var address = new Address { City = "NY" };

                        session.Store(address, "addresses/1");

                        var user = new User();

                        user.AddressId = address.Id;

                        session.Store(user, "users/1");

                        for (int i = 0; i < 10; i++)
                        {
                            session.TimeSeriesFor(user).Append("HeartRate", today.AddHours(i), address.Id, new double[] { 180 + i });
                        }

                        session.SaveChanges();
                    }

                    store.Maintenance.Send(new StopIndexingOperation());

                    var timeSeriesIndex = new AverageHeartRateDaily_ByDateAndCity();
                    var indexName = timeSeriesIndex.IndexName;
                    var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                    RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Users.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    ts = ts,\r\n    entry = entry\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    address = this.LoadDocument(this0.entry.Tag, \"Addresses\")\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.entry.Value,\r\n    Date = new DateTime((int) this1.this0.entry.TimeStamp.Date.Year, (int) this1.this0.entry.TimeStamp.Date.Month, (int) this1.this0.entry.TimeStamp.Date.Day),\r\n    City = this1.address.City,\r\n    Count = 1\r\n})", indexDefinition.Maps.First());
                    RavenTestHelper.AssertEqualRespectingNewLines("results.GroupBy(r => new {\r\n    Date = r.Date,\r\n    City = r.City\r\n}).Select(g => new {\r\n    g = g,\r\n    sumHeartBeat = Enumerable.Sum(g, x => ((double) x.HeartBeat))\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    sumCount = Enumerable.Sum(this0.g, x0 => ((long) x0.Count))\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.sumHeartBeat / ((double) this1.sumCount),\r\n    Date = this1.this0.g.Key.Date,\r\n    City = this1.this0.g.Key.City,\r\n    Count = this1.sumCount\r\n})", indexDefinition.Reduce);

                    timeSeriesIndex.Execute(store);

                    var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    WaitForIndexing(store);

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.False(staleness.IsStale);

                    var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("184.5", terms);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(today.Date.GetDefaultRavenFormat(), terms);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("ny", terms);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("10", terms[0]);

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        var address = session.Load<Address>("addresses/1");
                        address.City = "LA";

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    WaitForIndexing(store);

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.False(staleness.IsStale);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("la", terms);

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        session.Delete("addresses/1");

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    WaitForIndexing(store);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("NULL_VALUE", terms[0]);

                    // delete source document

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        session.Delete("users/1");

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    WaitForIndexing(store);

                    var database = await GetDatabase(store.Database);
                    var index = database.IndexStore.GetIndex(indexName);

                    using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = index._indexStorage.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(0, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);
                    }
                }
            }
        }

    }
}
