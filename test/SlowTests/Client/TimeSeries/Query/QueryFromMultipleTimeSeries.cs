using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries;
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out() 
{
    from Heartrate 
    between $start and $end
}
from Users as u
select out()
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out() 
{
    from Heartrate 
    between $start and $end
    group by 1h
    select min(), max(), avg()
}
from Users as u
select out()
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery_SelectSyntax()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
from Users as u
select timeseries(
    from Heartrate 
    between $start and $end
)")
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_SelectSyntax()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from Users as u
select timeseries(
    from Heartrate 
    between $start and $end
    group by 1h
    select min(), max(), avg()
)")
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_WithAliasNotation()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(doc) 
{
    from doc.Heartrate 
    between $start and $end
    group by 1h
    select min(), max(), avg()
}
from Users as u
select out(u)
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_UsingJsFunctionAsArgumentOfTimeSeriesFunction()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare function getName(doc)
    {
        return doc['@metadata']['@timeseries'][0];
    }
    declare timeseries out(name) 
    {
        from name between $start and $end
        group by 1h
        select min(), max()
    }
    from Users as u
    select out(getName(u))
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_ShouldReturnValidResults()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));
                var p = new TimeSeriesPolicy("By1Day", TimeSpan.FromHours(24), raw.RetentionTime * 5);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

                var baseline = now.AddDays(-3);
                var total = TimeSpan.FromDays(3).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    var tsf = session.TimeSeriesFor("users/karmel", "Heartrate");

                    for (int i = 0; i < total; i++)
                    {
                        tsf.Append(baseline.AddHours(i), i);
                    }

                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    // aggregated series have 6 different values per each original value (first, last, min, max, count, sum)
                    // when querying aggregated series we should apply the aggregation method (avg, min, max, etc.) only on the relevant value 

                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(doc) 
{
    from doc.Heartrate 
    between $start and $end
    group by 1day
    select avg(), max(), min(), first(), last()
}
from Users as u
select out(u)
")
                        .AddParameter("start", now.AddDays(-2))
                        .AddParameter("end", now.AddDays(-1));

                    var aggregationResult = query.First();

                    Assert.Equal(1, aggregationResult.Count);

                    var result = aggregationResult.Results[0];
                    Assert.Equal(1, result.Count.Length);
                    Assert.Equal(24, result.Count[0]);
                    Assert.Equal(1, result.First.Length);
                    Assert.Equal(1, result.Last.Length);
                    Assert.Equal(result.First, result.Min);
                    Assert.Equal(result.Last, result.Max);
                    Assert.Equal((result.First[0] + result.Last[0]) / 2, result.Average[0]);
                }

            }
        }

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery_PartialRange()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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


                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {

                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(name) 
{
    from name
    between $start and $end
}
from Users as u
select out('Heartrate')
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery_NoRetention()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"

declare timeseries out() 
{
    from Heartrate
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

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_PartialRange()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(name) 
{
    from name
    between $start and $end
    group by 1h
    select avg()
}
from Users as u
select out('Heartrate')
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
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_NoRetention()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"

declare timeseries out() 
{
    from Heartrate
    between $start and $end
    group by 1h
    select avg()
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

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery_UsingLinq()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => RavenQuery.TimeSeries(u, "Heartrate", baseline.AddDays(-1), now.AddDays(1))
                                .ToList());

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
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_UsingLinq()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>()
                        .Select(u => 
                            RavenQuery.TimeSeries(u, "Heartrate", baseline.AddDays(-1), now.AddDays(1))
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new
                            {
                                Avg = x.Average()
                            })
                            .ToList());

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
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery_UsingJsProjection()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel", LastName = "Indych" }, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var query =
                        from user in session.Query<User>()
                        let tsFunc = new Func<string, DateTime, DateTime, double?>((name, f, t) =>
                            RavenQuery.TimeSeries(user, name, f, t)
                                .ToList()
                                .Results
                                .Max(entry => entry.Values[0]))
                        select new
                        {
                            FullName = user.Name + " " + user.LastName,
                            TotalMax = tsFunc("Heartrate", baseline.AddDays(-1), now.AddDays(1))
                        };

                    var result = query.First();

                    Assert.Equal("Karmel Indych", result.FullName);
                    Assert.Equal(result.TotalMax, total);
                }
            }
        }

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_UsingJsProjection()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var series = "Heartrate";
                    var query = from user in session.Query<User>()
                                let customFunc = new Func<TimeSeriesRangeAggregation[], TimeSeriesJavascriptProjections.CustomJsFunctionResult2>(
                                    ranges => new TimeSeriesJavascriptProjections.CustomJsFunctionResult2
                                {
                                    TotalMax = ranges.Max(range => range.Max[0]),
                                    TotalMin = ranges.Min(range => range.Min[0]),
                                    AvgOfAvg = ranges.Average(range => range.Average[0]),
                                    MaxGroupSize = ranges.Max(r => r.Count[0])
                                })
                                let tsQuery = RavenQuery.TimeSeries(user, series, baseline.AddDays(-1), now.AddDays(1))
                                    .GroupBy(g => g.Hours(1))
                                    .Select(x => new
                                    {
                                        Max = x.Max(),
                                        Min = x.Min(),
                                        Avg = x.Average(),
                                        Count = x.Count()
                                    })
                                    .ToList()
                                select new
                                {
                                    Series = tsQuery,
                                    Custom = customFunc(tsQuery.Results)
                                };


                    var aggregationResult = query.First();

                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (2 * 24) // first day of 'By30Minutes'
                                   + 24 // first day of 'By1Hour'
                                   + 4  // first day of 'By6Hours'
                                   + 1; // first day of 'By24Hours'

                    Assert.Equal(expected, aggregationResult.Series.Count);

                    foreach (var rangeAggregation in aggregationResult.Series.Results)
                    {
                        Assert.Equal(1, rangeAggregation.Max.Length);
                        Assert.Equal(1, rangeAggregation.Min.Length);
                        Assert.Equal(1, rangeAggregation.Average.Length);
                        Assert.Equal(1, rangeAggregation.Count.Length);

                        var expectedAvg = (rangeAggregation.Max[0] + rangeAggregation.Min[0]) / 2;
                        Assert.Equal(rangeAggregation.Average[0], expectedAvg);
                    }

                    var totalMax = aggregationResult.Series.Results.Max(range => range.Max[0]);
                    var totalMin = aggregationResult.Series.Results.Min(range => range.Min[0]);
                    var avgOfAvg = aggregationResult.Series.Results.Average(range => range.Average[0]);
                    var maxGroupSize = aggregationResult.Series.Results.Max(range => range.Count[0]);

                    Assert.Equal(totalMax, aggregationResult.Custom.TotalMax);
                    Assert.Equal(totalMin, aggregationResult.Custom.TotalMin);
                    Assert.Equal(avgOfAvg, aggregationResult.Custom.AvgOfAvg);
                    Assert.Equal(maxGroupSize, aggregationResult.Custom.MaxGroupSize);
                }

            }
        }

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery_WithFilterByValue()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var name = "Heartrate";
                    var from = baseline.AddDays(-1);
                    var to = now;
                    var tenDaysInMinutes = TimeSpan.FromDays(10).TotalMinutes;

                    var query = session.Query<User>()
                        .Where(u => u.Id == "users/karmel")
                        .Select(u => RavenQuery.TimeSeries(u, name, from, to)
                            .Where(entry => entry.Value > tenDaysInMinutes)
                            .ToList());

                    var aggregationResult = query.First();

                    // only the last two days have Value > 14400
                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (2 * 24); // first day of 'By30Minutes'

                    Assert.Equal(expected, aggregationResult.Count);

                }

            }
        }

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_WithFilterByValue()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

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

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);


                using (var session = store.OpenSession())
                {
                    var name = "Heartrate";
                    var from = baseline.AddDays(-1);
                    var to = now;
                    var tenDaysInMinutes = TimeSpan.FromDays(10).TotalMinutes;

                    var query = session.Query<User>()
                        .Where(u => u.Id == "users/karmel")
                        .Select(u => RavenQuery.TimeSeries(u, name, from, to)
                            .Where(entry => entry.Value > tenDaysInMinutes)
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new
                            {
                                Max = x.Max(),
                                Min = x.Min(),
                                Avg = x.Average()
                            })
                            .ToList());

                    var aggregationResult = query.First();

                    // only the last two days have Value > 14400
                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (2 * 24); // first day of 'By30Minutes'

                    Assert.Equal(expected, aggregationResult.Count);

                }

            }
        }

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_RawQuery_WithFilterByValues()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    var tsf = session.TimeSeriesFor("users/karmel", "Heartrate");

                    for (int i = 0; i <= total; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new[] { 0d, i, 0 }, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var name = "Heartrate";
                    var from = baseline.AddDays(-1);
                    var to = now;
                    var tenDaysInMinutes = TimeSpan.FromDays(10).TotalMinutes;

                    var query = session.Query<User>()
                        .Where(u => u.Id == "users/karmel")
                        .Select(u => RavenQuery.TimeSeries(u, name, from, to)
                            .Where(entry => entry.Values[1] > tenDaysInMinutes)
                            .ToList());

                    var aggregationResult = query.First();

                    // only the last two days have Value > 14400
                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (2 * 24); // first day of 'By30Minutes'

                    Assert.Equal(expected, aggregationResult.Count);

                }
            }
        }

        [Fact]
        public async Task QueryFromMultipleTimeSeriesAtOnce_AggregationQuery_WithFilterByValues()
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
                var database = await GetDocumentDatabaseInstanceFor(store);

                var now = DateTime.UtcNow;
                var nowMinutes = now.Minute;
                now = now.AddMinutes(-nowMinutes);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(-nowMinutes);

                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/karmel");
                    var tsf = session.TimeSeriesFor("users/karmel", "Heartrate");

                    for (int i = 0; i <= total; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), new []{0d, i, 0}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await VerifyFullPolicyExecution(store, config.Collections["Users"]);

                using (var session = store.OpenSession())
                {
                    var name = "Heartrate";
                    var from = baseline.AddDays(-1);
                    var to = now;
                    var tenDaysInMinutes = TimeSpan.FromDays(10).TotalMinutes;

                    var query = session.Query<User>()
                        .Where(u => u.Id == "users/karmel")
                        .Select(u => RavenQuery.TimeSeries(u, name, from, to)
                            .Where(entry => entry.Values[1] > tenDaysInMinutes)
                            .GroupBy(g => g.Hours(1))
                            .Select(x => new
                            {
                                Max = x.Max(),
                                Min = x.Min(),
                                Avg = x.Average()
                            })
                            .ToList());

                    var aggregationResult = query.First();

                    // only the last two days have Value > 14400
                    var expected = (60 * 24) // entire raw policy for 1 day 
                                   + (2 * 24); // first day of 'By30Minutes'

                    Assert.Equal(expected, aggregationResult.Count);

                }

            }
        }

        internal static async Task VerifyFullPolicyExecution(DocumentStore store, TimeSeriesCollectionConfiguration configuration)
        {
            var raw = configuration.RawPolicy;
            configuration.ValidateAndInitialize();

            await WaitForValueAsync(() =>
            {
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue)?
                        .Where(entry => entry.IsRollup == false)
                        .ToList();

                    Assert.NotNull(ts);
                    Assert.Equal(((TimeSpan)raw.RetentionTime).TotalMinutes, ts.Count);

                    foreach (var policy in configuration.Policies)
                    {
                        ts = session.TimeSeriesFor("users/karmel", policy.GetTimeSeriesName("Heartrate"))
                            .Get(DateTime.MinValue, DateTime.MaxValue)?
                            .ToList();

                        TimeValue retentionTime = policy.RetentionTime;
                        if (retentionTime == TimeValue.MaxValue)
                        {
                            retentionTime = TimeSpan.FromDays(12);
                        }

                        Assert.NotNull(ts);
                        Assert.Equal(((TimeSpan)retentionTime).TotalMinutes / ((TimeSpan)policy.AggregationTime).TotalMinutes, ts.Count);
                    }
                }
                return true;
            }, true);
        }
    }
}
