using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.TimeSeries;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class QueryFromMultipleTimeSeries : RavenTestBase
    {
        public QueryFromMultipleTimeSeries(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryFromMultipleTimeSeriesAtOnce_AggregationQuery_DeclareSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 21);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 22);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 23);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate2");
                    tsf.Append(baseline.AddMinutes(71), 11);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 12);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 13);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 14);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 15);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 16);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate3");
                    tsf.Append(baseline.AddMinutes(61), 1);
                    tsf.Append(baseline.AddMinutes(62), 2);
                    tsf.Append(baseline.AddMinutes(63), 3);

                    tsf.Append(baseline.AddMinutes(71), 4);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 5);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 6);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 7);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 8);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 9);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from (Heartrate, Heartrate2, Heartrate3) between $start and $end
        group by 1h
        select min(), max()
    }
    from Users as u
    select out(u)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var aggResult = query.First();

                    Assert.Equal(9, aggResult.Count);

                    Assert.Equal(2, aggResult.Results.Length);

                    var aggregation = aggResult.Results[0];

                    Assert.Equal(4, aggregation.Count[0]);
                    Assert.Equal(1, aggregation.Min[0]);
                    Assert.Equal(11, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(1), aggregation.From);
                    Assert.Equal(baseline.AddHours(2), aggregation.To);

                    aggregation = aggResult.Results[1];

                    Assert.Equal(5, aggregation.Count[0]);
                    Assert.Equal(12, aggregation.Min[0]);
                    Assert.Equal(23, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(3), aggregation.From);
                    Assert.Equal(baseline.AddHours(4), aggregation.To);
                }
            }
        }

        [Fact]
        public void CanQueryFromMultipleTimeSeriesAtOnce_AggregationQuery_SelectSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 21);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 22);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 23);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate2");
                    tsf.Append(baseline.AddMinutes(71), 11);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 12);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 13);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 14);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 15);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 16);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate3");
                    tsf.Append(baseline.AddMinutes(61), 1);
                    tsf.Append(baseline.AddMinutes(62), 2);
                    tsf.Append(baseline.AddMinutes(63), 3);

                    tsf.Append(baseline.AddMinutes(71), 4);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 5);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 6);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 7);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 8);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 9);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Users as u
select timeseries(
    from (Heartrate, Heartrate2, Heartrate3) between $start and $end
    group by 1h
    select min(), max()
)")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var aggResult = query.First();

                    Assert.Equal(9, aggResult.Count);

                    Assert.Equal(2, aggResult.Results.Length);

                    var aggregation = aggResult.Results[0];

                    Assert.Equal(4, aggregation.Count[0]);
                    Assert.Equal(1, aggregation.Min[0]);
                    Assert.Equal(11, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(1), aggregation.From);
                    Assert.Equal(baseline.AddHours(2), aggregation.To);

                    aggregation = aggResult.Results[1];

                    Assert.Equal(5, aggregation.Count[0]);
                    Assert.Equal(12, aggregation.Min[0]);
                    Assert.Equal(23, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(3), aggregation.From);
                    Assert.Equal(baseline.AddHours(4), aggregation.To);
                }
            }
        }

        [Fact]
        public void CanQueryFromMultipleTimeSeriesAtOnce_RawQuery_DeclareSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 21);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 22);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 23);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate2");
                    tsf.Append(baseline.AddMinutes(71), 11);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 12);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 13);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 14);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 15);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 16);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate3");
                    tsf.Append(baseline.AddMinutes(61), 1);
                    tsf.Append(baseline.AddMinutes(62), 2);
                    tsf.Append(baseline.AddMinutes(63), 3);

                    tsf.Append(baseline.AddMinutes(71), 4);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 5);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 6);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 7);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 8);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 9);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
    declare timeseries out(u) 
    {
        from (Heartrate, Heartrate2, Heartrate3) 
        between $start and $end
        where Value != 200
    }
    from Users as u
    select out(u)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var rawResult = query.First();

                    Assert.Equal(9, rawResult.Count);

                    Assert.Equal(1, rawResult.Results[0].Value);
                    Assert.Equal(baseline.AddMinutes(61), rawResult.Results[0].Timestamp);

                    Assert.Equal(2, rawResult.Results[1].Value);
                    Assert.Equal(baseline.AddMinutes(62), rawResult.Results[1].Timestamp);

                    Assert.Equal(3, rawResult.Results[2].Value);
                    Assert.Equal(baseline.AddMinutes(63), rawResult.Results[2].Timestamp);

                    Assert.Equal(11, rawResult.Results[3].Value);
                    Assert.Equal(baseline.AddMinutes(71), rawResult.Results[3].Timestamp);

                    Assert.Equal(12, rawResult.Results[4].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(72), rawResult.Results[4].Timestamp);

                    Assert.Equal(13, rawResult.Results[5].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(73), rawResult.Results[5].Timestamp);

                    Assert.Equal(21, rawResult.Results[6].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(81), rawResult.Results[6].Timestamp);
                    
                    Assert.Equal(22, rawResult.Results[7].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(82), rawResult.Results[7].Timestamp);

                    Assert.Equal(23, rawResult.Results[8].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(83), rawResult.Results[8].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryFromMultipleTimeSeriesAtOnce_RawQuery_SelectSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 21);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 22);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 23);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate2");
                    tsf.Append(baseline.AddMinutes(71), 11);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 12);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 13);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 14);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 15);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 16);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate3");
                    tsf.Append(baseline.AddMinutes(61), 1);
                    tsf.Append(baseline.AddMinutes(62), 2);
                    tsf.Append(baseline.AddMinutes(63), 3);

                    tsf.Append(baseline.AddMinutes(71), 4);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 5);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 6);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 7);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 8);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 9);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
from Users as u
select timeseries(
    from (Heartrate, Heartrate2, Heartrate3) 
    between $start and $end
    where Value != 200
)")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var rawResult = query.First();

                    Assert.Equal(9, rawResult.Count);

                    Assert.Equal(1, rawResult.Results[0].Value);
                    Assert.Equal(baseline.AddMinutes(61), rawResult.Results[0].Timestamp);

                    Assert.Equal(2, rawResult.Results[1].Value);
                    Assert.Equal(baseline.AddMinutes(62), rawResult.Results[1].Timestamp);

                    Assert.Equal(3, rawResult.Results[2].Value);
                    Assert.Equal(baseline.AddMinutes(63), rawResult.Results[2].Timestamp);

                    Assert.Equal(11, rawResult.Results[3].Value);
                    Assert.Equal(baseline.AddMinutes(71), rawResult.Results[3].Timestamp);

                    Assert.Equal(12, rawResult.Results[4].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(72), rawResult.Results[4].Timestamp);

                    Assert.Equal(13, rawResult.Results[5].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(73), rawResult.Results[5].Timestamp);

                    Assert.Equal(21, rawResult.Results[6].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(81), rawResult.Results[6].Timestamp);

                    Assert.Equal(22, rawResult.Results[7].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(82), rawResult.Results[7].Timestamp);

                    Assert.Equal(23, rawResult.Results[8].Value);
                    Assert.Equal(baseline.AddHours(2).AddMinutes(83), rawResult.Results[8].Timestamp);

                }
            }
        }

        [Fact]
        public void CanQueryFromMultipleTimeSeriesAtOnce_AggregationQuery_DeclareSyntax_WithAliasNotation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 21);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 22);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 23);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate2");
                    tsf.Append(baseline.AddMinutes(71), 11);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 12);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 13);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 14);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 15);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 16);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate3");
                    tsf.Append(baseline.AddMinutes(61), 1);
                    tsf.Append(baseline.AddMinutes(62), 2);
                    tsf.Append(baseline.AddMinutes(63), 3);

                    tsf.Append(baseline.AddMinutes(71), 4);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 5);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 6);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 7);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 8);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 9);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from (u.Heartrate, u.Heartrate2, u.Heartrate3) between $start and $end
        group by 1h
        select min(), max()
    }
    from Users as u
    select out(u)
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var aggResult = query.First();

                    Assert.Equal(9, aggResult.Count);

                    Assert.Equal(2, aggResult.Results.Length);

                    var aggregation = aggResult.Results[0];

                    Assert.Equal(4, aggregation.Count[0]);
                    Assert.Equal(1, aggregation.Min[0]);
                    Assert.Equal(11, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(1), aggregation.From);
                    Assert.Equal(baseline.AddHours(2), aggregation.To);

                    aggregation = aggResult.Results[1];

                    Assert.Equal(5, aggregation.Count[0]);
                    Assert.Equal(12, aggregation.Min[0]);
                    Assert.Equal(23, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(3), aggregation.From);
                    Assert.Equal(baseline.AddHours(4), aggregation.To);
                }
            }
        }

        [Fact]
        public void CanQueryFromMultipleTimeSeriesAtOnce_WithSeriesNamesAsListParameter_UsingJsFunctionAsArgumentOfTimeSeriesFunction()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 21);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 22);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 23);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate2");
                    tsf.Append(baseline.AddMinutes(71), 11);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 12);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 13);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 14);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 15);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 16);

                    tsf = session.TimeSeriesFor("users/ayende", "Heartrate3");
                    tsf.Append(baseline.AddMinutes(61), 1);
                    tsf.Append(baseline.AddMinutes(62), 2);
                    tsf.Append(baseline.AddMinutes(63), 3);

                    tsf.Append(baseline.AddMinutes(71), 4);
                    tsf.Append(baseline.AddHours(2).AddMinutes(72), 5);
                    tsf.Append(baseline.AddHours(2).AddMinutes(73), 6);

                    tsf.Append(baseline.AddHours(2).AddMinutes(81), 7);
                    tsf.Append(baseline.AddHours(2).AddMinutes(82), 8);
                    tsf.Append(baseline.AddHours(2).AddMinutes(83), 9);

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare function getTsNames(doc)
    {
        return doc['@metadata']['@timeseries'];
    }
    declare timeseries out(names) 
    {
        from names between $start and $end
        group by 1h
        select min(), max()
    }
    from Users as u
    select out(getTsNames(u))
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));

                    var aggResult = query.First();

                    Assert.Equal(9, aggResult.Count);

                    Assert.Equal(2, aggResult.Results.Length);

                    var aggregation = aggResult.Results[0];

                    Assert.Equal(4, aggregation.Count[0]);
                    Assert.Equal(1, aggregation.Min[0]);
                    Assert.Equal(11, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(1), aggregation.From);
                    Assert.Equal(baseline.AddHours(2), aggregation.To);

                    aggregation = aggResult.Results[1];

                    Assert.Equal(5, aggregation.Count[0]);
                    Assert.Equal(12, aggregation.Min[0]);
                    Assert.Equal(23, aggregation.Max[0]);

                    Assert.Equal(baseline.AddHours(3), aggregation.From);
                    Assert.Equal(baseline.AddHours(4), aggregation.To);
                }
            }
        }

        [Fact]
        public async Task CanQueryFromMultipleTimeSeriesAtOnce_AggregationQuery_OverRollUps()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare function getTsNames(doc)
{
    return doc['@metadata']['@timeseries'];
}
declare timeseries out(names) 
{
    from names 
    between $start and $end
    group by 1h
    select min(), max(), avg()
}
from Users as u
select out(getTsNames(u))
")
                        .AddParameter("start", baseline.AddDays(-1))
                        .AddParameter("end", now.AddDays(1));

                    var aggregationResult = query.First();

                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (2 * 24) // first day of 'By30Minutes'
                                   + 24 // first day of 'By1Hour'
                                   + 4  // first day of 'By6Hours'
                                   + 1; // first day of 'By24Hours'

                    Assert.Equal(expected, aggregationResult.Count);


                }

            }
        }

        [Fact]
        public async Task CanQueryFromMultipleTimeSeriesAtOnce_RawQuery_OverRollUps()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {

                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare function getTsNames(doc)
{
    return doc['@metadata']['@timeseries'];
}
declare timeseries out(names) 
{
    from names 
    between $start and $end
}
from Users as u
select out(getTsNames(u))
")
                        .AddParameter("start", baseline.AddDays(-1))
                        .AddParameter("end", now.AddDays(1));

                    var result = query.First();

                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (2 * 24) // first day of 'By30Minutes'
                                   + 24 // first day of 'By1Hour'
                                   + 4  // first day of 'By6Hours'
                                   + 1; // first day of 'By1Day'

                    Assert.Equal(expected, result.Count);


                }

            }
        }

        [Fact]
        public async Task CanQueryFromMultipleTimeSeriesAtOnce_RawQuery_OverRollUps_PartialRange()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {

                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare function getTsNames(doc)
{
    return ['Heartrate', 'Heartrate_By30Minutes', 'Heartrate_By1Hour', 'Heartrate_By6Hours', 'Heartrate_By1Day'];
}
declare timeseries out(names) 
{
    from names 
    between $start and $end
}
from Users as u
select out(getTsNames(u))
")
                        .AddParameter("start", now.AddDays(-3))
                        .AddParameter("end", now.AddDays(-1));

                    var result = query.First();

                    var expected = (2 * 24) // first day of 'By30Minutes'
                                   + 24; // first day of 'By1Hour for 3Days'


                    Assert.Equal(expected, result.Count);


                }

            }
        }

        [Fact]
        public async Task CanQueryFromMultipleTimeSeriesAtOnce_RawQuery_OverRollUps_NoRetention()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours", TimeSpan.FromHours(6));
                var p2 = new TimeSeriesPolicy("By1Day", TimeSpan.FromDays(1));
                var p3 = new TimeSeriesPolicy("By30Minutes", TimeSpan.FromMinutes(30));
                var p4 = new TimeSeriesPolicy("By1Hour", TimeSpan.FromMinutes(60));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"

declare timeseries out() 
{
    from (Heartrate, Heartrate_By30Minutes, Heartrate_By1Hour, Heartrate_By6Hours, Heartrate_By1Day) 
    between $start and $end
}
from Users as u
select out()
")
                        .AddParameter("start", now.AddDays(-7))
                        .AddParameter("end", now);

                    var result = query.First();

                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (6 * (2 * 24)); // 6 days of 'By30Minutes'

                    Assert.Equal(expected, result.Count);


                }

            }
        }

        private async Task VerifyFullPolicyExecution(DocumentStore store, TimeSeriesCollectionConfiguration configuration)
        {
            var raw = configuration.RawPolicy;
            configuration.ValidateAndInitialize();

            await WaitForValueAsync(() =>
            {
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(((TimeSpan)raw.RetentionTime).TotalMinutes, ts.Count);

                    foreach (var policy in configuration.Policies)
                    {
                        ts = session.TimeSeriesFor("users/karmel", policy.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                        TimeValue retentionTime = policy.RetentionTime;
                        if (retentionTime == TimeValue.MaxValue)
                        {
                            retentionTime = TimeSpan.FromDays(12);
                        }

                        Assert.Equal(((TimeSpan)retentionTime).TotalMinutes / ((TimeSpan)policy.AggregationTime).TotalMinutes, ts.Count);
                    }
                }
                return true;
            }, true);
        }
    }
}
