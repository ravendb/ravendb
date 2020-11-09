using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.Indexing.Counters;
using Sparrow.Json;
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
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

        private class MyTsIndex_WithSpace : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public MyTsIndex_WithSpace()
            {
                AddMap(
                    "Heart Rate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

        private class MyTsIndex_AllTimeSeries : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public MyTsIndex_AllTimeSeries()
            {
                AddMapForAll(timeSeries => from ts in timeSeries
                                           from entry in ts.Entries
                                           select new
                                           {
                                               HeartBeat = entry.Values[0],
                                               entry.Timestamp.Date,
                                               User = ts.DocumentId
                                           });
            }
        }

        private class MyTsIndex_AllDocs : AbstractTimeSeriesIndexCreationTask<object>
        {
            public class Result
            {
                public string Name { get; set; }

                public DateTime Date { get; set; }
            }

            public MyTsIndex_AllDocs()
            {
                AddMapForAll(timeSeries => from ts in timeSeries
                                           from entry in ts.Entries
                                           select new
                                           {
                                               HeartBeat = entry.Value,
                                               Name = ts.Name,
                                               entry.Timestamp.Date,
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
                                      entry.Timestamp.Date,
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
                                      Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day),
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
                                                      Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day),
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

        private class MyMultiMapTsIndex : AbstractMultiMapTimeSeriesIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Date { get; set; }

                public string User { get; set; }
            }

            public MyMultiMapTsIndex()
            {
                AddMap<Company>(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });

                AddMap<Company>(
                    "HeartRate2",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });

                AddMap<User>(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

        private class MyMultiMapTsIndex_Load : AbstractMultiMapTimeSeriesIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Date { get; set; }

                public string User { get; set; }
            }

            public MyMultiMapTsIndex_Load()
            {
                AddMap<Company>(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let address = LoadDocument<Address>("addresses/" + entry.Value)
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });

                AddMap<Company>(
                    "HeartRate2",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let address = LoadDocument<Address>("addresses/" + entry.Value)
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });

                AddMap<User>(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let address = LoadDocument<Address>("addresses/" + entry.Value)
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

        private class Companies_ByTimeSeriesNames : AbstractIndexCreationTask<Company>
        {
            public Companies_ByTimeSeriesNames()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       Names = TimeSeriesNamesFor(company)
                                   };
            }
        }

        [Fact]
        public void BasicMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyTsIndex();
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Companies.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Values[0],\r\n    Date = entry.Timestamp.Date,\r\n    User = ts.DocumentId\r\n})", indexDefinition.Maps.First());

                store.ExecuteIndex(timeSeriesIndex);

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
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 3 }, "tag");

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
                Assert.Equal(now1.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);

                // delete time series

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now2);

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
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 9 }, "tag");

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

                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, employee.Id);

                    var company2 = new Company();
                    session.Store(company2, "companies/11");

                    session.TimeSeriesFor(company2, "HeartRate").Append(now1, new double[] { 11 }, employee.Id);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyTsIndex_Load();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Companies.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    ts = ts,\r\n    entry = entry\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    employee = this.LoadDocument(this0.entry.Tag, \"Employees\")\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.entry.Value,\r\n    Date = this1.this0.entry.Timestamp.Date,\r\n    User = this1.this0.ts.DocumentId,\r\n    Employee = this1.employee.FirstName\r\n})", indexDefinition.Maps.First());

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
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

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
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

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
                var today = RavenTestHelper.UtcToday;
                var tomorrow = today.AddDays(1);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, "users/1");

                    for (int i = 0; i < 10; i++)
                    {
                        session.TimeSeriesFor(user, "HeartRate").Append(today.AddHours(i), new double[] { 180 + i }, "abc");
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new AverageHeartRateDaily_ByDateAndUser();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Users.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Value,\r\n    Date = new DateTime((int) entry.Timestamp.Date.Year, (int) entry.Timestamp.Date.Month, (int) entry.Timestamp.Date.Day),\r\n    User = ts.DocumentId,\r\n    Count = 1\r\n})", indexDefinition.Maps.First());
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
                Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

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
                        session.TimeSeriesFor(user, "HeartRate").Append(tomorrow.AddHours(i), new double[] { 200 + i }, "abc");
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
                Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(tomorrow.Date, DateTime.Parse(terms[1]), RavenTestHelper.DateTimeComparer.Instance);

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
                        session.TimeSeriesFor(user, "HeartRate").Delete(today.AddHours(i));
                        session.TimeSeriesFor(user, "HeartRate").Delete(tomorrow.AddHours(i));
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
                Assert.Equal(tomorrow.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

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
                    var today = RavenTestHelper.UtcToday;

                    using (var session = store.OpenSession())
                    {
                        var address = new Address { City = "NY" };

                        session.Store(address, "addresses/1");

                        var user = new User();

                        user.AddressId = address.Id;

                        session.Store(user, "users/1");

                        for (int i = 0; i < 10; i++)
                        {
                            session.TimeSeriesFor(user, "HeartRate").Append(today.AddHours(i), new double[] { 180 + i }, address.Id);
                        }

                        session.SaveChanges();
                    }

                    store.Maintenance.Send(new StopIndexingOperation());

                    var timeSeriesIndex = new AverageHeartRateDaily_ByDateAndCity();
                    var indexName = timeSeriesIndex.IndexName;
                    var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                    RavenTestHelper.AssertEqualRespectingNewLines("timeSeries.Users.HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    ts = ts,\r\n    entry = entry\r\n}).Select(this0 => new {\r\n    this0 = this0,\r\n    address = this.LoadDocument(this0.entry.Tag, \"Addresses\")\r\n}).Select(this1 => new {\r\n    HeartBeat = this1.this0.entry.Value,\r\n    Date = new DateTime((int) this1.this0.entry.Timestamp.Date.Year, (int) this1.this0.entry.Timestamp.Date.Month, (int) this1.this0.entry.Timestamp.Date.Day),\r\n    City = this1.address.City,\r\n    Count = 1\r\n})", indexDefinition.Maps.First());
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
                    Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

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
                        var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(0, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);
                    }
                }
            }
        }

        [Fact]
        public void CanMapAllTimeSeriesFromCollection()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag1");
                    session.TimeSeriesFor(company, "Likes").Append(now1, new double[] { 3 }, "tag2");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                new MyTsIndex_AllTimeSeries().Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 2 }, "tag");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("2", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now1);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("3", terms);
                Assert.Contains("2", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now2);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("3", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void CanMapAllTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag1");
                    session.TimeSeriesFor(company, "Likes").Append(now1, new double[] { 3 }, "tag2");

                    var employee = new Employee();
                    session.Store(employee, "employees/1");
                    session.TimeSeriesFor(employee, "Dislikes").Append(now1, new double[] { 1 }, "tag3");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                new MyTsIndex_AllDocs().Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 2 }, "tag");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(4, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "Likes").Delete(now1, now2);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("1", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("dislikes", terms);

                // now checking live updates (not stopping indexing)

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("employees/1");
                    session.TimeSeriesFor(company, "Dislikes").Append(now2, new double[] { 9 }, "tag");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("1", terms);
                Assert.Contains("9", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("dislikes", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllDocs"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public async Task BasicMultiMapIndex()
        {
            var now = DateTime.UtcNow.Date;

            using (var store = GetDocumentStore())
            {
                var timeSeriesIndex = new MyMultiMapTsIndex();
                await timeSeriesIndex.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company);

                    session.TimeSeriesFor(company, "HeartRate").Append(now, new[] { 2.5d }, "tag1");
                    session.TimeSeriesFor(company, "HeartRate2").Append(now, new[] { 3.5d }, "tag2");

                    var user = new User();
                    session.Store(user);
                    session.TimeSeriesFor(user, "HeartRate").Append(now, new[] { 4.5d }, "tag3");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Query<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToListAsync();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Advanced.AsyncDocumentQuery<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToListAsync();

                    Assert.Equal(3, results.Count);
                }
            }
        }

        [Fact]
        public void SupportForEscapedCollectionAndTimeSeriesNames()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = t =>
                {
                    if (t == typeof(Company))
                        return "Companies With Space";

                    return null;
                }
            }))
            {
                var timeSeriesIndex = new MyTsIndex_WithSpace();
                timeSeriesIndex.Conventions = store.Conventions;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("timeSeries[@ \"Companies With Space\"][@ \"Heart Rate\"].SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Values[0],\r\n    Date = entry.Timestamp.Date,\r\n    User = ts.DocumentId\r\n})", indexDefinition.Maps.First());

                AssertIndex(store, indexDefinition, "Heart Rate");
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = t =>
                {
                    if (t == typeof(Company))
                        return "Companies With Space";

                    return null;
                }
            }))
            {
                var timeSeriesIndex = new MyTsIndex_WithSpace();
                timeSeriesIndex.Conventions = store.Conventions;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                indexDefinition.Maps = new HashSet<string> {
                    "timeSeries[@ \"Companies With Space\"].HeartRate.SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Values[0],\r\n    Date = entry.Timestamp.Date,\r\n    User = ts.DocumentId\r\n})"
                };

                AssertIndex(store, indexDefinition, "HeartRate");

                indexDefinition.Maps = new HashSet<string> {
                    "timeSeries[@ \"Companies With Space\"].SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Values[0],\r\n    Date = entry.Timestamp.Date,\r\n    User = ts.DocumentId\r\n})"
                };

                AssertIndex(store, indexDefinition, "HeartRate");
            }

            using (var store = GetDocumentStore())
            {
                var timeSeriesIndex = new MyTsIndex_WithSpace();
                timeSeriesIndex.Conventions = store.Conventions;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                indexDefinition.Maps = new HashSet<string> {
                    "timeSeries.Companies[@ \"Heart Rate\"].SelectMany(ts => ts.Entries, (ts, entry) => new {\r\n    HeartBeat = entry.Values[0],\r\n    Date = entry.Timestamp.Date,\r\n    User = ts.DocumentId\r\n})"
                };

                AssertIndex(store, indexDefinition, "Heart Rate");
            }

            static void AssertIndex(IDocumentStore store, IndexDefinition definition, string timeSeriesName)
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, timeSeriesName).Append(now1, new double[] { 7 }, "tag");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                store.Maintenance.Send(new PutIndexesOperation(definition));

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, timeSeriesName).Append(now2, new double[] { 3 }, "tag");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new DeleteIndexOperation(definition.Name));
            }
        }

        [Fact]
        public void TimeSeriesNamesFor()
        {
            var now = DateTime.UtcNow.Date;

            using (var store = GetDocumentStore())
            {
                var index = new Companies_ByTimeSeriesNames();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company);

                    session.TimeSeriesFor(company, "HeartRate").Append(now, new[] { 2.5d }, "tag1");
                    session.TimeSeriesFor(company, "HeartRate2").Append(now, new[] { 3.5d }, "tag2");

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("heartrate2", terms);
            }
        }

        [Fact]
        public async Task CanCalculateNumberOfReferencesCorrectly()
        {
            using (var store = GetDocumentStore())
            {
                var countersIndex = new MyMultiMapTsIndex_Load();
                var indexName = countersIndex.IndexName;
                await countersIndex.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    var address1 = new Address { Id = "addresses/1" };
                    var address2 = new Address { Id = "addresses/11" };
                    var address3 = new Address { Id = "addresses/2" };

                    session.Store(address1);
                    session.Store(address2);
                    session.Store(address3);

                    var company = new Company();
                    session.Store(company, "companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.UtcNow, new double[] { 1 });
                    session.TimeSeriesFor(company, "HeartRate2").Append(DateTime.UtcNow, new double[] { 11 });

                    var user = new User();
                    session.Store(user, "companies/11");
                    session.TimeSeriesFor(user, "HeartRate").Append(DateTime.UtcNow, new double[] { 1 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                var indexInstance = database.IndexStore.GetIndex(indexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(3, counts.ReferenceTableCount);
                    Assert.Equal(2, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Users", tx);

                    Assert.Equal(3, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Users", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/11");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);

                    counts = indexInstance._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Users", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [Fact]
        public void ShouldUseOriginalTimeSeriesName()
        {
            using (var store = GetDocumentStore())
            {
                var index = new MyTsIndex_AllDocs();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "U1" };

                    session.Store(user);

                    session.TimeSeriesFor(user, "UPPER").Append(DateTime.UtcNow, 1);
                    session.TimeSeriesFor(user, "lower").Append(DateTime.UtcNow, 2);
                    session.TimeSeriesFor(user, "mIxEd").Append(DateTime.UtcNow, 3);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyTsIndex_AllDocs.Result, MyTsIndex_AllDocs>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                    Assert.Contains("UPPER", results.Select(x => x.Name));
                    Assert.Contains("lower", results.Select(x => x.Name));
                    Assert.Contains("mIxEd", results.Select(x => x.Name));

                    Assert.True(results.All(x => x.Date.Kind == DateTimeKind.Utc));
                }
            }
        }

        private class ProgressTestTimeSeriesIndex : AbstractMultiMapTimeSeriesIndexCreationTask
        {
            public ProgressTestTimeSeriesIndex()
            {
                AddMap<User>(
                    $"TimeSeries",
                    timeSeriesSegments => 
                        from seriesSegment in timeSeriesSegments
                        select new { Value = seriesSegment.Entries.Average(e => e.Value) });   
            }
        }

        private class ProgressTestTimeSeriesMapReduceIndex : AbstractTimeSeriesIndexCreationTask<User, ProgressTestTimeSeriesMapReduceIndex.Result>
        {
            public class Result
            {
                public double Max { get; set; }
                public bool IsBig { get; set; }
            }
            public ProgressTestTimeSeriesMapReduceIndex()
            {
                AddMap($"TimeSeries",
                    timeSeriesSegments => 
                        from timeSeriesSegment in timeSeriesSegments
                        let max = timeSeriesSegment.Entries.Max(e => e.Value)
                        select new
                        {
                            Max = max,
                            IsBig = max > 20 
                        });

                Reduce = results =>
                    from result in results
                    group result by new {result.IsBig}
                    into g
                    select new {IsBig = g.Key, Max = g.Max(r => r.Max)};
            }
        }


        public static IEnumerable<object[]> ProgressTestIndexes =>
            new[]
            {
                new object[] {new ProgressTestTimeSeriesIndex()}, 
                new object[] {new ProgressTestTimeSeriesMapReduceIndex()},
            };
        
        [Theory]
        [MemberData(nameof(ProgressTestIndexes))]
        public async Task TimeSeriesIndexProgress_WhenMapMultipleSegment_ShouldDisplayNumberOfSegmentToMap(AbstractTimeSeriesIndexCreationTask index)
        {
            const int numberOfTimeSeries = 500 * 1000;
            
            using var store = GetDocumentStore();

            await index.ExecuteAsync(store);

            var user = new User();
            var baseTime = new DateTime(2020, 11, 8);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);

                for (var i = 0; i < numberOfTimeSeries; i++)
                {
                    session.TimeSeriesFor(user.Id, "TimeSeries").Append(baseTime.AddMilliseconds(i), 12);
                }
                await session.SaveChangesAsync();
            }
            WaitForIndexing(store);

            await store.Maintenance.SendAsync(new StopIndexingOperation());
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                var start = new DateTime(2020, 11, 9);
                for (var i = 0; i < numberOfTimeSeries; i++)
                {
                    session.TimeSeriesFor(user.Id, "TimeSeries").Append(start.AddMilliseconds(i), 12);
                }
                await session.SaveChangesAsync();
                
                var progress = await GetProgressAsync(store);
                Assert.True(progress.NumberOfItemsToProcess > 1);
                Assert.True(progress.TotalNumberOfItems > 1);
            }
            
            await store.Maintenance.SendAsync(new StartIndexingOperation());
            WaitForIndexing(store);
            await store.Maintenance.SendAsync(new StopIndexingOperation());
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                session.TimeSeriesFor(user.Id, "TimeSeries").Delete(baseTime, baseTime.AddMilliseconds(numberOfTimeSeries));
                await session.SaveChangesAsync();
                
                var progress = await GetProgressAsync(store);
                Assert.True(progress.NumberOfItemsToProcess > 1);
                Assert.True(progress.TotalNumberOfItems > 1);
            }
        }

        private static async Task<IndexProgress.CollectionStats> GetProgressAsync(IDocumentStore store)
        {
            using var context = JsonOperationContext.ShortTermSingleUse();
            var client = store.GetRequestExecutor(store.Database).HttpClient;
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get,
                RequestUri = new Uri($"{store.Urls[0]}/databases/{store.Database}/indexes/progress")
            };
            var response = await client.SendAsync(request);
            response.EnsureSuccessStatusCode();
            var content = await response.Content.ReadAsStringAsync();
            var indexesProgress = JsonConvert.DeserializeObject<IndexesProgress>(content);
            if (indexesProgress.Results.Count > 0 && indexesProgress.Results[0].Collections.ContainsKey("Users"))
            {
                return indexesProgress.Results[0].Collections["Users"];
            }
            return null;
        }
    }
}
