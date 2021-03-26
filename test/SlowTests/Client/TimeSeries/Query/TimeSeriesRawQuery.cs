using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Query
{
    public class TimeSeriesRawQuery : RavenTestBase
    {
        public TimeSeriesRawQuery(ITestOutputHelper output) : base(output)
        {
        }

        private class PeopleIndex : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex()
            {
                Map = people => from person in people
                                select new
                                {
                                    person.Age
                                };
            }

            public override string IndexName => "People";
        }

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from u in users
                               select new
                                {
                                    u.Age
                                };
            }
        }

        private class Person
        {
            public string Name { get; set; }

            public int Age { get; set; }

            public string WorksAt { get; set; }

            public string Event { get; set;  }

            public AdditionalData AdditionalData { get; set;  }

        }

        private class AdditionalData
        {
            public NestedClass NestedClass { get; set; }
        }

        private class NestedClass
        {
            public Event Event { get; set; }

            public double Accuracy { get; set; }
        }

        private class Event
        {
            public DateTime Start { get; set; }

            public DateTime End { get; set; }

            public string Description { get; set; }

        }

        private class Watch
        {
            public string Manufacturer { get; set;  }

            public double Accuracy { get; set;  }

            public AdditionalData AdditionalData { get; set; }

            public long Min { get; set; }

            public long Max { get; set; }

            public bool IsoCompliant;

            public DateTime EndOfWarranty { get; set; }

        }


        public class RawQueryResult
        {
            public TimeSeriesAggregationResult HeartRate { get; set; }

            public TimeSeriesAggregationResult BloodPressure { get; set; }

            public TimeSeriesAggregationResult Stocks { get; set; }

            public string Name { get; set; }
        }

        private struct StockPrice
        {
            [TimeSeriesValue(0)] public double Open;
            [TimeSeriesValue(1)] public double Close;
            [TimeSeriesValue(2)] public double High;
            [TimeSeriesValue(3)] public double Low;
            [TimeSeriesValue(4)] public double Volume;
        }

        [Fact]
        public unsafe void CanQueryTimeSeriesAggregation_DeclareSyntax_AllDocsQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from @all_docs as u
    where id() == 'users/ayende'
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var agg = query.First();

                    if (agg.Count != 3)
                    {
                        var db = GetDocumentDatabaseInstanceFor(store).Result;
                        var tss = db.DocumentsStorage.TimeSeriesStorage;
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var reader = tss.GetReader(ctx, "users/ayende", "Heartrate", baseline, baseline.AddDays(1));

                            Assert.True(reader.Init());

                            Assert.NotNull(reader._tvr);

                            var key = reader._tvr.Read(0, out var size);

                            TimeSeriesValuesSegment.ParseTimeSeriesKey(key, size, ctx, out var docId, out var name, out DateTime baseline2);

                            Assert.Equal("users/ayende", docId);
                            Assert.Equal("Heartrate", name);
                            Assert.Equal(baseline.AddMinutes(61), baseline2);

                            Assert.Equal(1, reader.SegmentsOrValues().Count());

                            Assert.False(query.First().Count == 3, "Query assertion failed once and passed on second try. sanity check passed");

                            //Assert.True(false, "Query assertion failed twice. sanity check passed");
                        }
                    }

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First[0]);
                    Assert.Equal(59, val.Min[0]);

                    Assert.Equal(69, val.Last[0]);
                    Assert.Equal(79, val.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from Users as u
    where id() == 'users/ayende'
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var agg = query.First();

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First[0]);
                    Assert.Equal(59, val.Min[0]);

                    Assert.Equal(69, val.Last[0]);
                    Assert.Equal(79, val.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_DynamicIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren", Age = 50}, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from Users as u
    where u.Age > 49
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var agg = query.First();

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First[0]);
                    Assert.Equal(59, val.Min[0]);

                    Assert.Equal(69, val.Last[0]);
                    Assert.Equal(79, val.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Oren", Age = 50 }, "users/ayende");

                    var tsf = session.TimeSeriesFor("users/ayende", "Heartrate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                    session.SaveChanges();
                }

                new UsersIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
    declare timeseries out(u) 
    {
        from u.Heartrate between $start and $end
        group by 1h
        select min(), max(), first(), last()
    }
    from index 'UsersIndex' as u
    where u.Age > 49
    select out(u)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var agg = query.First();

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.First[0]);
                    Assert.Equal(59, val.Min[0]);

                    Assert.Equal(69, val.Last[0]);
                    Assert.Equal(79, val.Max[0]);

                    Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_WithOtherFields()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        session.SaveChanges();
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
declare timeseries out(p) 
{
    from p.HeartRate between $start and $end 
    group by 1h 
    select min(), max()
}
from index 'People' as p 
where p.Age > 49
select out(p) as HeartRate, p.Name 
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal("Oren", agg.Name);

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_MultipleSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var baseline2 = DateTime.Today.AddDays(-1);


                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf = session.TimeSeriesFor(id, "BloodPressure");
                        tsf.Append(baseline2.AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline2.AddMinutes(62), new[] { 179d }, "watches/apple");
                        tsf.Append(baseline2.AddMinutes(63), new[] { 168d }, "watches/apple");
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
declare timeseries heart_rate(doc) 
{
    from doc.HeartRate between $start and $end
    group by 1h 
    select min(), max()
}
declare timeseries blood_pressure(doc) 
{
    from doc.BloodPressure between $start2 and $end2 
    group by 1h 
    select min(), max(), avg()
}
from index 'People' as p 
where p.Age > 49
select heart_rate(p) as HeartRate, blood_pressure(p) as BloodPressure
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc())
                        .AddParameter("start2", baseline2.EnsureUtc())
                        .AddParameter("end2", baseline2.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);

                        var bloodPressure = agg.BloodPressure;

                        Assert.Equal(3, bloodPressure.Count);

                        Assert.Equal(1, bloodPressure.Results.Length);

                        val = bloodPressure.Results[0];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);

                        double expectedAvg = (159 + 168 + 179) / 3.0;

                        Assert.Equal(expectedAvg, val.Average[0]);

                        Assert.Equal(baseline2.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline2.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DeclareSyntax_FromLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        var company = $"companies/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = company
                        }, id);

                        session.Store(new Company(), company);

                        var tsf = session.TimeSeriesFor(company, "Stocks");

                        tsf.Append(baseline.AddMinutes(61), new[] { 1259.51d }, "tag");
                        tsf.Append(baseline.AddMinutes(62), new[] { 1279.62d }, "tag");
                        tsf.Append(baseline.AddMinutes(63), new[] { 1269.73d }, "tag");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(c) 
{
    from c.Stocks between $start and $end
    group by 1h 
    select min(), max(), avg()
}
from People as p
where p.Age > 49
load p.WorksAt as Company
select out(Company)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1259.51d, val.Min[0]);
                        Assert.Equal(1279.62d, val.Max[0]);

                        double expectedAvg = (1259.51d + 1279.62d + 1269.73d) / 3.0;
                        Assert.Equal(expectedAvg, val.Average[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_CollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People as p 
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(3, result.Count);

                    for (int i = 0; i < 3; i++)
                    {
                        var agg = result[i];
                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_DynamicIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People as p 
where p.Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];
                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_StaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];
                        Assert.Equal(3, agg.Count);

                        Assert.Equal(1, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_AsAlias()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
as HeartRate
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];
                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_WithOtherFields()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end 
    group by 1h 
    select min(), max())
as HeartRate, Name 
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal("Oren", agg.Name);

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_MultipleSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                var baseline2 = DateTime.Today.AddDays(-1);


                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf = session.TimeSeriesFor(id, "BloodPressure");
                        tsf.Append(baseline2.AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline2.AddMinutes(62), new[] { 179d }, "watches/apple");
                        tsf.Append(baseline2.AddMinutes(63), new[] { 168d }, "watches/apple");
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from index 'People'
where Age > 49
select timeseries(
    from HeartRate between $start and $end
    group by 1h 
    select min(), max())
as HeartRate, timeseries(
    from BloodPressure between $start2 and $end2 
    group by 1h 
    select min(), max(), avg())
as BloodPressure
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc())
                        .AddParameter("start2", baseline2.EnsureUtc())
                        .AddParameter("end2", baseline2.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);

                        var bloodPressure = agg.BloodPressure;

                        Assert.Equal(3, bloodPressure.Count);

                        Assert.Equal(1, bloodPressure.Results.Length);

                        val = bloodPressure.Results[0];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);

                        double expectedAvg = (159 + 168 + 179) / 3.0;

                        Assert.Equal(expectedAvg, val.Average[0]);

                        Assert.Equal(baseline2.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline2.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_AliasNotation()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "Heartrate");
                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from People as p
where p.Age > 49
select timeseries(
    from p.HeartRate between $start and $end
    group by 1h 
    select min(), max())
as HeartRate
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var heartrate = agg.HeartRate;

                        Assert.Equal(3, heartrate.Count);

                        Assert.Equal(1, heartrate.Results.Length);

                        var val = heartrate.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_FromLoadedDocument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        var company = $"companies/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = company
                        }, id);

                        session.Store(new Company(), company);

                        var tsf = session.TimeSeriesFor(company, "Stocks");

                        tsf.Append(baseline.AddMinutes(61), new[] { 1259.51d }, "tag");
                        tsf.Append(baseline.AddMinutes(62), new[] { 1279.62d }, "tag");
                        tsf.Append(baseline.AddMinutes(63), new[] { 1269.73d }, "tag");
                    }

                    session.SaveChanges();
                }

                new PeopleIndex().Execute(store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<RawQueryResult>(@"
from People as p
where p.Age > 49
load p.WorksAt as Company
select timeseries(
    from Company.Stocks between $start and $end
    group by 1h 
    select min(), max(), avg())
as Stocks
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        var stocks = agg.Stocks;

                        Assert.Equal(3, stocks.Count);

                        Assert.Equal(1, stocks.Results.Length);

                        var val = stocks.Results[0];

                        Assert.Equal(1259.51d, val.Min[0]);
                        Assert.Equal(1279.62d, val.Max[0]);

                        double expectedAvg = (1259.51d + 1279.62d + 1269.73d) / 3.0;
                        Assert.Equal(expectedAvg, val.Average[0]);

                        Assert.Equal(baseline.AddMinutes(60), val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(baseline.AddMinutes(120), val.To, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectSyntax_WhereOnTagOrValue()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/sony");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People as doc
where doc.Age > 49
select timeseries(from doc.HeartRate between $start and $end
        where Values[0] < 70 or Tag = 'watches/fitbit'
    group by '1 month' 
    select min(), max(), avg())
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }


        [Theory]
        [InlineData("months")]
        [InlineData("month")]
        [InlineData("mon")]
        [InlineData("mo")]
        public void CanQueryTimeSeriesAggregation_GroupByMonth(string syntax)
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>($@"
declare timeseries out(x) 
{{
    from x.HeartRate between $start and $end
    group by '1 {syntax}' 
    select min(), max(), avg()
}}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_NoBetween()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate
    group by '1 month' 
    select min(), max()
}
from People as doc
where doc.Age > 49
select out(doc)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(59, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(79, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(159, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                        
                        val = agg.Results[4];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[5];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_NoSelectOrGroupBy_MultipleValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d, 159 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d, 179 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d, 259 }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d, 269 }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(2, val.Values.Length);
                        Assert.Equal(59, val.Values[0]);
                        Assert.Equal(159, val.Values[1]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(2, val.Values.Length);
                        Assert.Equal(79, val.Values[0]);
                        Assert.Equal(179, val.Values[1]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[3];

                        Assert.Equal(2, val.Values.Length);
                        Assert.Equal(159, val.Values[0]);
                        Assert.Equal(259, val.Values[1]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[4];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[5];

                        Assert.Equal(2, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal(269, val.Values[1]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_MultipleParameters()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {

                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            Event = "events/" + i
                        }, id);

                        session.Store(new Event
                        {
                            Start = baseline.AddDays(i - 1),
                            End = baseline.AddDays(i - 1).AddMonths(2)
                        }, "events/" + i);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(p, e) 
{
    from p.HeartRate between $start and $end
    group by '1 month' 
    select min(), max()
}
from People as doc
where doc.Age > 49
load doc.Event as e
select out(doc, e)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithMultipleValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d, 159 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d, 179 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d, 169 }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d, 259 }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d, 279 }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d, 269 }, "watches/apple");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries heart_rate(doc) 
{
    from doc.HeartRate between $start and $end
    group by '1 month'
    select min(), max(), avg()
}
from People as p 
where p.Age > 49
select heart_rate(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(6, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(2, val.Min.Length);
                        Assert.Equal(2, val.Max.Length);
                        Assert.Equal(2, val.Average.Length);

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);

                        Assert.Equal(159, val.Min[1]);
                        Assert.Equal(179, val.Max[1]);
                        Assert.Equal(169, val.Average[1]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(2, val.Min.Length);
                        Assert.Equal(2, val.Max.Length);
                        Assert.Equal(2, val.Average.Length);

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        Assert.Equal(259, val.Min[1]);
                        Assert.Equal(279, val.Max[1]);
                        Assert.Equal(269, val.Average[1]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_DifferentNumberOfValues()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d, 179 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d, 169, 269 }, "watches/fitbit");

                        tsf.Append(baseline.AddDays(1).AddMinutes(61), new[] { 159d, 259 }, "watches/apple");
                        tsf.Append(baseline.AddDays(1).AddMinutes(62), new[] { 179d, 279, 379 }, "watches/apple");
                        tsf.Append(baseline.AddDays(1).AddMinutes(63), new[] { 169d }, "watches/apple");

                        tsf.Append(baseline.AddDays(2).AddMinutes(61), new[] { 259d, 359, 459 }, "watches/apple");
                        tsf.Append(baseline.AddDays(2).AddMinutes(62), new[] { 279d }, "watches/apple");
                        tsf.Append(baseline.AddDays(2).AddMinutes(63), new[] { 269d, 369, 469, 569 }, "watches/apple");
                    }

                    session.SaveChanges();
                }

                var offset = TimeZoneInfo.Local.BaseUtcOffset.ToString(@"hh\:mm");
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>($@"
declare timeseries heart_rate(doc) 
{{
    from doc.HeartRate between $start and $end
    group by '1 day'
    select min(), max(), avg(), first(), last()
    offset '{offset}'
}}
from People as p 
where p.Age > 49
select heart_rate(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(3).EnsureUtc());


                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(9, agg.Count);

                        Assert.Equal(3, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(3, val.Min.Length);
                        Assert.Equal(3, val.Max.Length);
                        Assert.Equal(3, val.Average.Length);
                        Assert.Equal(3, val.First.Length);
                        Assert.Equal(3, val.Last.Length);

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);
                        Assert.Equal(59, val.First[0]);
                        Assert.Equal(69, val.Last[0]);
                        Assert.Equal(3, val.Count[0]);

                        Assert.Equal(169, val.Min[1]);
                        Assert.Equal(179, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((169 + 179) / 2, val.Average[1]);
                        Assert.Equal(179, val.First[1]);
                        Assert.Equal(169, val.Last[1]);

                        Assert.Equal(269, val.Min[2]);
                        Assert.Equal(269, val.Max[2]);
                        Assert.Equal(1, val.Count[2]);
                        Assert.Equal(269, val.Average[2]);
                        Assert.Equal(269, val.First[2]);
                        Assert.Equal(269, val.Last[2]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, baseline.Day, 0, 0, 0);
                        var expectedTo = expectedFrom.AddDays(1);

                        Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(3, val.Min.Length);
                        Assert.Equal(3, val.Max.Length);
                        Assert.Equal(3, val.Average.Length);
                        Assert.Equal(3, val.First.Length);
                        Assert.Equal(3, val.Last.Length);

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);
                        Assert.Equal(159, val.First[0]);
                        Assert.Equal(169, val.Last[0]);
                        Assert.Equal(3, val.Count[0]);

                        Assert.Equal(259, val.Min[1]);
                        Assert.Equal(279, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((259d + 279) / 2, val.Average[1]);
                        Assert.Equal(259, val.First[1]);
                        Assert.Equal(279, val.Last[1]);

                        Assert.Equal(379, val.Min[2]);
                        Assert.Equal(379, val.Max[2]);
                        Assert.Equal(1, val.Count[2]);
                        Assert.Equal(379, val.Average[2]);
                        Assert.Equal(379, val.First[2]);
                        Assert.Equal(379, val.Last[2]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddDays(1);

                        Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(4, val.Min.Length);
                        Assert.Equal(4, val.Max.Length);
                        Assert.Equal(4, val.Average.Length);
                        Assert.Equal(4, val.First.Length);
                        Assert.Equal(4, val.Last.Length);

                        Assert.Equal(259, val.Min[0]);
                        Assert.Equal(279, val.Max[0]);
                        Assert.Equal(269, val.Average[0]);
                        Assert.Equal(259, val.First[0]);
                        Assert.Equal(269, val.Last[0]);
                        Assert.Equal(3, val.Count[0]);

                        Assert.Equal(359, val.Min[1]);
                        Assert.Equal(369, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((359d + 369) / 2, val.Average[1]);
                        Assert.Equal(359, val.First[1]);
                        Assert.Equal(369, val.Last[1]);

                        Assert.Equal(459, val.Min[2]);
                        Assert.Equal(469, val.Max[2]);
                        Assert.Equal(2, val.Count[2]);
                        Assert.Equal((459d + 469) / 2, val.Average[2]);
                        Assert.Equal(459, val.First[2]);
                        Assert.Equal(469, val.Last[2]);

                        Assert.Equal(569, val.Min[3]);
                        Assert.Equal(569, val.Max[3]);
                        Assert.Equal(1, val.Count[3]);
                        Assert.Equal(569, val.Average[3]);
                        Assert.Equal(569, val.First[3]);
                        Assert.Equal(569, val.Last[3]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddDays(1);

                        Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                        Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTag_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate
        where Tag == 'watches/fitbit'
}
from People as doc
where doc.Age > 49
select out(doc)
");
                    
                    var result = query.ToList();
              
                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(59, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(69, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnValue_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate
        where Values[0] > 70
}
from People as doc
where doc.Age > 49
select out(doc)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(79, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(159, val.Values[0]);
                        Assert.Equal("watches/apple", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(61), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[2];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[3];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTagAndValue_NoSelectOrGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
from People as doc
where doc.Age > 49
select timeseries(from doc.HeartRate where Tag == 'watches/fitbit' and Values[0] > 70)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(179, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(62), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                        val = agg.Results[1];

                        Assert.Equal(1, val.Values.Length);
                        Assert.Equal(169, val.Values[0]);
                        Assert.Equal("watches/fitbit", val.Tag);
                        Assert.Equal(baseline.AddMonths(1).AddMinutes(63), val.Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Tag == 'watches/fitbit'
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnValue()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] > 70
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTagOrValue()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/sony");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] < 70 or Tag = 'watches/fitbit'
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereNotNull()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d, 141.5 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d, 142.82 }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d, 138.12 }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d, 142.57 }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[1] != null
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);

                        Assert.Equal(141.5, val.Min[1]);
                        Assert.Equal(142.82, val.Max[1]);
                        Assert.Equal((141.5 + 142.82) / 2, val.Average[1]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(169, val.Max[0]);
                        Assert.Equal(164, val.Average[0]);

                        Assert.Equal(138.12, val.Min[1]);
                        Assert.Equal(142.57, val.Max[1]);
                        Assert.Equal((138.12 + 142.57) / 2, val.Average[1]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOrNot()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d, 141.5 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d, 142.82 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d, 138.12 }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d, 142.57 }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[1] != null OR NOT Tag = 'watches/fitbit'
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(5, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);
                        Assert.Equal(3, val.Count[0]);

                        Assert.Equal(141.5, val.Min[1]);
                        Assert.Equal(142.82, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((141.5 + 142.82) / 2, val.Average[1]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(169, val.Max[0]);
                        Assert.Equal(164, val.Average[0]);
                        Assert.Equal(2, val.Count[0]);

                        Assert.Equal(138.12, val.Min[1]);
                        Assert.Equal(142.57, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((138.12 + 142.57) / 2, val.Average[1]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereAndNot()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d, 141.5 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d, 142.82 }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/apple");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d, 138.12 }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d, 142.57 }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[1] != null AND NOT Tag = 'watches/apple'
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());
                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);
                        Assert.Equal(2, val.Count[0]);

                        Assert.Equal(141.5, val.Min[1]);
                        Assert.Equal(142.82, val.Max[1]);
                        Assert.Equal(2, val.Count[1]);
                        Assert.Equal((141.5 + 142.82) / 2, val.Average[1]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(169, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);
                        Assert.Equal(1, val.Count[0]);

                        Assert.Equal(142.57, val.Min[1]);
                        Assert.Equal(142.57, val.Max[1]);
                        Assert.Equal(142.57, val.Average[1]);
                        Assert.Equal(1, val.Count[1]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnQueryParameter()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] > $val
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc())
                        .AddParameter("val", 70);

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnFunctionArgument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR",
                        AccountsReceivable = (decimal)70.7
                    }, "companies/1");

                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = "companies/1"
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x, val) 
{
    from x.HeartRate between $start and $end
        where Values[0] > val
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
load doc.WorksAt as c
select out(doc, c.AccountsReceivable)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnLoadedDocumentArgument()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR",
                        AccountsReceivable = (decimal)70.7
                    }, "companies/1");

                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";
                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = "companies/1"
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x, y) 
{
    from x.HeartRate between $start and $end
        where Values[0] > y.AccountsReceivable
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
load doc.WorksAt as c
select out(doc, c)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereIn()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Tag in ('watches/fitbit', 'watches/apple')
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(5, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(169, val.Max[0]);
                        Assert.Equal(164, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereInNumbers()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] in (50, 59, 79, 99, 159, 179)
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);
                        Assert.Equal(2, val.Count[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);
                        Assert.Equal(2, val.Count[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8
                    }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);
                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src    
        where src.Accuracy > 2
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(169, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnNestedPropertyFromLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        AdditionalData = new AdditionalData
                        {
                            NestedClass = new NestedClass
                            {
                                Accuracy = 2.5
                            }
                        }
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        AdditionalData = new AdditionalData
                        {
                            NestedClass = new NestedClass
                            {
                                Accuracy = 2.135
                            }
                        }
                    }, "watches/apple");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src    
        where src.AdditionalData.NestedClass.Accuracy > 2.15
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(169, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnBoolean()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5,
                        IsoCompliant = true
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8,
                        IsoCompliant = false
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Accuracy = 2.8,
                        IsoCompliant = true
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src    
        where src.IsoCompliant = true
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTimestamp()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5,
                        EndOfWarranty = baseline.AddDays(-1)
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8,
                        EndOfWarranty = baseline.AddMonths(6)
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Accuracy = 2.8,
                        EndOfWarranty = baseline.AddDays(2)
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src    
        where TimeStamp <= src.EndOfWarranty
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(2, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(159, val.Max[0]);
                        Assert.Equal(159, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereOnTimestamp2()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5,
                        EndOfWarranty = baseline.AddDays(-1)
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8,
                        EndOfWarranty = baseline.AddMonths(6)
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Accuracy = 2.8,
                        EndOfWarranty = baseline.AddDays(2)
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src    
        where src.EndOfWarranty >= TimeStamp
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(2, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(159, val.Max[0]);
                        Assert.Equal(159, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereInOnPropertyFromLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Accuracy = 2.8
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where src.Manufacturer in ('Apple', 'Sony')
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereInNumbersFromLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Accuracy = 2.8
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where src.Accuracy in (2, 2.25, 2.5, 2.8, 3)
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);
                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);
                        Assert.Equal(2, val.Count[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);
                        Assert.Equal(2, val.Count[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereBetween()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        where Values[0] between 70 and 170
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(79, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(79, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(169, val.Max[0]);
                        Assert.Equal(164, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereBetweenOnPropertyFromLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Accuracy = 2.8
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where src.Accuracy between 2.2 and 2.8
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereBetweenOnPropertyFromLoadedTagAndQueryParameters()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Accuracy = 2.5
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Accuracy = 1.8
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Accuracy = 2.8
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where src.Accuracy between $minVal and $maxVal
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc())
                        .AddParameter("minVal", 2.2)
                        .AddParameter("maxVal", 2.8);

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(59, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(64, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereBetweenOnValueAndPropertiesFromLoadedTag()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Min = 60,
                        Max = 160
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Min = 70,
                        Max = 170
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Min = 75,
                        Max = 185
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where Values[0] between src.Min and src.Max
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(4, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(69, val.Min[0]);
                        Assert.Equal(79, val.Max[0]);
                        Assert.Equal(74, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(159, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(169, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_ComplexWhereWithSubclauses()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        Manufacturer = "Fitbit",
                        Min = 60,
                        Max = 160,
                        IsoCompliant = true
                    }, "watches/fitbit");

                    session.Store(new Watch
                    {
                        Manufacturer = "Apple",
                        Min = 70,
                        Max = 170
                    }, "watches/apple");

                    session.Store(new Watch
                    {
                        Manufacturer = "Sony",
                        Min = 75,
                        Max = 185,
                        IsoCompliant = true
                    }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where (Tag = 'watches/fitbit' AND Values[0] between src.Min and src.Max) OR 
              (src.IsoCompliant = true AND Values[0] > $val)  
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc().EnsureUtc())
                        .AddParameter("val", 100d);

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    for (int i = 0; i < 2; i++)
                    {
                        var agg = result[i];

                        Assert.Equal(3, agg.Count);

                        Assert.Equal(2, agg.Results.Length);

                        var val = agg.Results[0];

                        Assert.Equal(69, val.Min[0]);
                        Assert.Equal(69, val.Max[0]);
                        Assert.Equal(69, val.Average[0]);

                        var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                        var expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);

                        val = agg.Results[1];

                        Assert.Equal(169, val.Min[0]);
                        Assert.Equal(179, val.Max[0]);
                        Assert.Equal(174, val.Average[0]);

                        expectedFrom = expectedTo;
                        expectedTo = expectedFrom.AddMonths(1);

                        Assert.Equal(expectedFrom, val.From);
                        Assert.Equal(expectedTo, val.To);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithFieldExpressionInBetweenClause()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Event
                        {
                            Start = baseline.AddMonths(i - 1).EnsureUtc(),
                            End = baseline.AddMonths(3).EnsureUtc()
                        }, $"events/{i}");

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            Event = $"events/{i}"
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(2).AddMinutes(61), new[] { 259d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(2).AddMinutes(62), new[] { 279d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(2).AddMinutes(63), new[] { 269d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x, e) 
{
    from x.HeartRate between e.Start and e.End
    group by '1 hour' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
load doc.Event as e
select out(doc, e)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    var agg = result[0];

                    Assert.Equal(6, agg.Count);

                    Assert.Equal(2, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(159, val.Min[0]);
                    Assert.Equal(179, val.Max[0]);
                    Assert.Equal(169, val.Average[0]);

                    var expectedFrom = baseline.AddMonths(1).AddHours(1);
                    var expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                    val = agg.Results[1];

                    Assert.Equal(259, val.Min[0]);
                    Assert.Equal(279, val.Max[0]);
                    Assert.Equal(269, val.Average[0]);

                    expectedFrom = baseline.AddMonths(2).AddHours(1);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                    agg = result[1];

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    val = agg.Results[0];

                    Assert.Equal(259, val.Min[0]);
                    Assert.Equal(279, val.Max[0]);
                    Assert.Equal(269, val.Average[0]);

                    expectedFrom = baseline.AddMonths(2).AddHours(1);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeries_BetweenWithFieldAndValue()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var id = $"people/1";

                    session.Store(new Event
                    {
                        Start = baseline.EnsureUtc(),
                        End = baseline.AddMonths(3).EnsureUtc()
                    }, $"events/1");

                    session.Store(new Person
                    {
                        Name = "dardasaba", 
                        Age = 120,
                        Event = $"events/1"
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");
                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x, e) 
{
    from x.HeartRate between e.Start and '9999'
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
load doc.Event as e
select out(doc, e)
");

                    var result = query.ToList();
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithNestedFieldExpressionInBetweenClause()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        var e = new Event
                        {
                            Start = baseline.AddMonths(i - 1).EnsureUtc(), 
                            End = baseline.AddMonths(3).EnsureUtc()
                        };

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            AdditionalData = new AdditionalData
                            {
                                NestedClass = new NestedClass
                                {
                                    Event = e
                                }
                            }
                        }, id);

                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(2).AddMinutes(61), new[] { 259d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(2).AddMinutes(62), new[] { 279d }, "watches/sony");
                        tsf.Append(baseline.AddMonths(2).AddMinutes(63), new[] { 269d }, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between x.AdditionalData.NestedClass.Event.Start and x.AdditionalData.NestedClass.Event.End
    group by '1 hour' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
select out(doc)
");

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);
                    var agg = result[0];

                    Assert.Equal(6, agg.Count);

                    Assert.Equal(2, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(159, val.Min[0]);
                    Assert.Equal(179, val.Max[0]);
                    Assert.Equal(169, val.Average[0]);

                    var expectedFrom = baseline.AddMonths(1).AddHours(1);
                    var expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                    val = agg.Results[1];

                    Assert.Equal(259, val.Min[0]);
                    Assert.Equal(279, val.Max[0]);
                    Assert.Equal(269, val.Average[0]);

                    expectedFrom = baseline.AddMonths(2).AddHours(1);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);

                    agg = result[1];

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    val = agg.Results[0];

                    Assert.Equal(259, val.Min[0]);
                    Assert.Equal(279, val.Max[0]);
                    Assert.Equal(269, val.Average[0]);

                    expectedFrom = baseline.AddMonths(2).AddHours(1);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, val.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, val.To, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithComparisonBetweenValueAndFunctionArgumentInWhereClause()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 3; i++)
                    {
                        var id = $"people/{i}";

                        session.Store(new Person
                        {
                            Name = "Oren",
                            Age = i * 30,
                            WorksAt = $"companies/{i}"
                        }, id);

                        session.Store(new Company
                        {
                            AccountsReceivable = (decimal)30.3 * i
                        }, $"companies/{i}");
                        var tsf = session.TimeSeriesFor(id, "HeartRate");

                        tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                        tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                        tsf.Append(baseline.AddMinutes(63), new[] { 69d }, "watches/fitbit");

                        tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/fitbit");
                        tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x, e) 
{
    from x.HeartRate between $start and $end
        where Values[0] > e.AccountsReceivable
    group by '1 month' 
    select min(), max(), avg()
}
from People as doc
where doc.Age > 49
load doc.WorksAt as c
select out(doc, c)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var result = query.ToList();

                    Assert.Equal(2, result.Count);

                    var agg = result[0];

                    Assert.Equal(5, agg.Count);

                    Assert.Equal(2, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(69, val.Min[0]);
                    Assert.Equal(79, val.Max[0]);
                    Assert.Equal(74, val.Average[0]);

                    var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                    var expectedTo = expectedFrom.AddMonths(1);

                    Assert.Equal(expectedFrom, val.From);
                    Assert.Equal(expectedTo, val.To);

                    val = agg.Results[1];

                    Assert.Equal(159, val.Min[0]);
                    Assert.Equal(179, val.Max[0]);
                    Assert.Equal(169, val.Average[0]);

                    expectedFrom = expectedTo;
                    expectedTo = expectedFrom.AddMonths(1);

                    Assert.Equal(expectedFrom, val.From);
                    Assert.Equal(expectedTo, val.To);

                    agg = result[1];

                    Assert.Equal(3, agg.Count);

                    Assert.Equal(1, agg.Results.Length);

                    val = agg.Results[0];
                    
                    Assert.Equal(159, val.Min[0]);
                    Assert.Equal(179, val.Max[0]);
                    Assert.Equal(169, val.Average[0]);

                    expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0).AddMonths(1);
                    expectedTo = expectedFrom.AddMonths(1);

                    Assert.Equal(expectedFrom, val.From);
                    Assert.Equal(expectedTo, val.To);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WhereLoadedTagNotNull()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var id = $"people/1";

                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 30,
                    }, id);

                    session.Store(new Watch
                    {
                        Accuracy = 2.5
                    }, "watches/fitbit");
                    session.Store(new Watch
                    {
                        Accuracy = 1.8
                    }, "watches/apple");

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), new[] { 69d });

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where src != null and src.Accuracy > 2
    group by '1 month' 
    select min(), max(), avg()
}
from People as p
select out(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());


                    var agg = query.First();

                    Assert.Equal(2, agg.Count);

                    Assert.Equal(2, agg.Results.Length);

                    var val = agg.Results[0];

                    Assert.Equal(59, val.Min[0]);
                    Assert.Equal(59, val.Max[0]);
                    Assert.Equal(59, val.Average[0]);

                    var expectedFrom = new DateTime(baseline.Year, baseline.Month, 1, 0, 0, 0);
                    var expectedTo = expectedFrom.AddMonths(1);

                    Assert.Equal(expectedFrom, val.From);
                    Assert.Equal(expectedTo, val.To);

                    val = agg.Results[1];

                    Assert.Equal(169, val.Min[0]);
                    Assert.Equal(169, val.Max[0]);
                    Assert.Equal(169, val.Average[0]);

                    expectedFrom = expectedTo;
                    expectedTo = expectedFrom.AddMonths(1);

                    Assert.Equal(expectedFrom, val.From);
                    Assert.Equal(expectedTo, val.To);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnInvalidOperationInsideWhere()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Watch
                    {
                        EndOfWarranty = baseline.AddYears(1)
                    }, "watches/fitbit");
                    session.Store(new Person
                    {
                        Name = "Oren"
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1", "HeartRate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
        load Tag as src
        where Values[0] > src.EndOfWarranty
    group by '1 month' 
    select max()
}
from People as p
select out(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(2).EnsureUtc());

                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());
                    Assert.Contains("Operator '>' cannot be applied to operands of type 'double' and 'Sparrow.Json.LazyStringValue'", ex.Message);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_SelectWithoutGroupBy()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var id = "people/1";

                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 30,
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), new[] { 369d }, null);

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                var from = baseline.EnsureUtc();
                var to = baseline.AddMonths(2).EnsureUtc();
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
    where Tag != null
    select max()
}
from People as p
select out(p)
")
                        .AddParameter("start", from)
                        .AddParameter("end", to);

                    var agg = query.First();

                    Assert.Equal(5, agg.Count);

                    Assert.Equal(179, agg.Results[0].Max[0]);
                    Assert.Equal(from, agg.Results[0].From);
                    Assert.Equal(to, agg.Results[0].To);

                }
            }
        }

         [Fact]
        public void CanQueryTimeSeriesAggregation_SelectWithoutGroupBy_FullRange()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var id = "people/1";

                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 30,
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline.AddMinutes(61), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(62), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(63), new[] { 369d }, null);

                    tsf.Append(baseline.AddMonths(1).AddMinutes(61), new[] { 159d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(62), new[] { 179d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(63), new[] { 169d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate
    where Tag != null
    select max()
}
from People as p
select out(p)
");

                    var agg = query.First();

                    Assert.Equal(5, agg.Count);

                    Assert.Equal(179, agg.Results[0].Max[0]);
                    Assert.Equal(DateTime.MinValue, agg.Results[0].From);
                    Assert.Equal(DateTime.MaxValue, agg.Results[0].To);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var person = new Person
                    {
                        Name = "Oren"
                    };

                    session.Store(person);

                    var tsf = session.TimeSeriesFor(person, "HeartRate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/sony");

                    tsf.Append(baseline.AddHours(1).AddMinutes(1), new[] { 159d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(1).AddMinutes(2), new[] { 179d }, "watches/apple");
                    tsf.Append(baseline.AddHours(1).AddMinutes(3), new[] { 169d }, "watches/sony");

                    tsf.Append(baseline.AddHours(2).AddMinutes(1), new[] { 259d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(2).AddMinutes(2), new[] { 279d }, "watches/apple");
                    tsf.Append(baseline.AddHours(2).AddMinutes(3),new[] { 269d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(1), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(2), new[] { 379d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(3), new[] { 369d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(1), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(2), new[] { 479d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(3), new[] { 469d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(1), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(2), new[] { 579d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(3), new[] { 569d }, "watches/fitbit");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
    group by '1h' 
    select min(), max()
    offset '02:00'
}
from People as p
select out(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(6).EnsureUtc());

                    var agg = query.First();

                    Assert.Equal(12, agg.Count);

                    Assert.Equal(4, agg.Results.Length);

                    var rangeAggregation = agg.Results[0];

                    Assert.Equal(59, rangeAggregation.Min[0]);
                    Assert.Equal(79, rangeAggregation.Max[0]);

                    var offset = TimeSpan.FromHours(2);

                    var expectedFrom = baseline.Add(offset);
                    var expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[1];

                    Assert.Equal(159, rangeAggregation.Min[0]);
                    Assert.Equal(179, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[2];

                    Assert.Equal(259, rangeAggregation.Min[0]);
                    Assert.Equal(279, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(2).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[3];

                    Assert.Equal(359, rangeAggregation.Min[0]);
                    Assert.Equal(379, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddMonths(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithNegativeOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var id = $"people/1";

                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 30,
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Append(baseline.AddHours(-3).AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(-3).AddMinutes(2), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddHours(-3).AddMinutes(3), new[] { 69d }, "watches/sony");

                    tsf.Append(baseline.AddHours(-2).AddMinutes(1), new[] { 159d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(-2).AddMinutes(2), new[] { 179d }, "watches/apple");
                    tsf.Append(baseline.AddHours(-2).AddMinutes(3), new[] { 169d }, "watches/sony");

                    tsf.Append(baseline.AddMinutes(1), new[] { 259d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 279d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 269d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(1), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(2), new[] { 379d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(3), new[] { 369d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(-3).AddMinutes(1), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(-3).AddMinutes(2), new[] { 479d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(-3).AddMinutes(3), new[] { 469d }, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
    group by '1h' 
    select min(), max()
    offset '-02:00'
}
from People as p
select out(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(6).EnsureUtc());

                    var agg = query.First();

                    var offset = TimeSpan.FromHours(-2);

                    Assert.Equal(9, agg.Count);

                    Assert.Equal(3, agg.Results.Length);

                    var rangeAggregation = agg.Results[0];

                    Assert.Equal(259, rangeAggregation.Min[0]);
                    Assert.Equal(279, rangeAggregation.Max[0]);

                    var expectedFrom = baseline.Add(offset);
                    var expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[1];

                    Assert.Equal(359, rangeAggregation.Min[0]);
                    Assert.Equal(379, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddMonths(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[2];

                    Assert.Equal(459, rangeAggregation.Min[0]);
                    Assert.Equal(479, rangeAggregation.Max[0]);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    expectedFrom = baseline.AddMonths(6).AddHours(-3).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_WithOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = new DateTime(2019, 1, 1);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                        Age = 30,
                    }, "people/1");

                    var tsf = session.TimeSeriesFor("people/1", "HeartRate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/sony");

                    tsf.Append(baseline.AddHours(1).AddMinutes(1), new[] { 159d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(1).AddMinutes(2), new[] { 179d }, "watches/apple");
                    tsf.Append(baseline.AddHours(1).AddMinutes(3), new[] { 169d }, "watches/sony");

                    tsf.Append(baseline.AddDays(2).AddMinutes(1), new[] { 259d }, "watches/fitbit");
                    tsf.Append(baseline.AddDays(2).AddMinutes(2), new[] { 279d }, "watches/apple");
                    tsf.Append(baseline.AddDays(2).AddMinutes(3), new[] { 269d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(6).AddMinutes(1), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(2), new[] { 579d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddMinutes(3), new[] { 569d }, "watches/fitbit");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
    offset '02:00'
}
from People as p
select out(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddYears(1).EnsureUtc());

                    var result = query.First();

                    Assert.Equal(12, result.Count);

                    var baselineWithOffset = baseline.Add(TimeSpan.FromHours(2));
                    Assert.Equal(baselineWithOffset.AddMinutes(1), result.Results[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[0].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddMinutes(2), result.Results[1].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[1].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddMinutes(3), result.Results[2].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[2].Timestamp.Kind);

                    Assert.Equal(baselineWithOffset.AddHours(1).AddMinutes(1), result.Results[3].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[3].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddHours(1).AddMinutes(2), result.Results[4].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[4].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddHours(1).AddMinutes(3), result.Results[5].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[5].Timestamp.Kind);

                    Assert.Equal(baselineWithOffset.AddDays(2).AddMinutes(1), result.Results[6].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[6].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddDays(2).AddMinutes(2), result.Results[7].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[7].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddDays(2).AddMinutes(3), result.Results[8].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[8].Timestamp.Kind);

                    Assert.Equal(baselineWithOffset.AddMonths(6).AddMinutes(1), result.Results[9].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[9].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddMonths(6).AddMinutes(2), result.Results[10].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[10].Timestamp.Kind);
                    Assert.Equal(baselineWithOffset.AddMonths(6).AddMinutes(3), result.Results[11].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, result.Results[11].Timestamp.Kind);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithOffset_AsQueryParameter_String()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var person = new Person
                    {
                        Name = "Oren"
                    };

                    session.Store(person);

                    var tsf = session.TimeSeriesFor(person, "HeartRate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/sony");

                    tsf.Append(baseline.AddHours(1).AddMinutes(1), new[] { 159d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(1).AddMinutes(2), new[] { 179d }, "watches/apple");
                    tsf.Append(baseline.AddHours(1).AddMinutes(3), new[] { 169d }, "watches/sony");

                    tsf.Append(baseline.AddHours(2).AddMinutes(1), new[] { 259d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(2).AddMinutes(2), new[] { 279d }, "watches/apple");
                    tsf.Append(baseline.AddHours(2).AddMinutes(3), new[] { 269d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(1), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(2), new[] { 379d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(3), new[] { 369d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(1), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(2), new[] { 479d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(3), new[] { 469d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(1), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(2), new[] { 579d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(3), new[] { 569d }, "watches/fitbit");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
    group by '1h' 
    select min(), max()
    offset $offset
}
from People as p
select out(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(6).EnsureUtc())
                        .AddParameter("offset", "03:00");

                    var agg = query.First();

                    Assert.Equal(12, agg.Count);

                    Assert.Equal(4, agg.Results.Length);

                    var rangeAggregation = agg.Results[0];

                    Assert.Equal(59, rangeAggregation.Min[0]);
                    Assert.Equal(79, rangeAggregation.Max[0]);

                    var offset = TimeSpan.FromHours(3);

                    var expectedFrom = baseline.Add(offset);
                    var expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[1];

                    Assert.Equal(159, rangeAggregation.Min[0]);
                    Assert.Equal(179, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[2];

                    Assert.Equal(259, rangeAggregation.Min[0]);
                    Assert.Equal(279, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(2).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[3];

                    Assert.Equal(359, rangeAggregation.Min[0]);
                    Assert.Equal(379, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddMonths(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithOffset_AsQueryParameter_TimeSpan()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var person = new Person
                    {
                        Name = "Oren"
                    };

                    session.Store(person);

                    var tsf = session.TimeSeriesFor(person, "HeartRate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/sony");

                    tsf.Append(baseline.AddHours(1).AddMinutes(1), new[] { 159d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(1).AddMinutes(2), new[] { 179d }, "watches/apple");
                    tsf.Append(baseline.AddHours(1).AddMinutes(3), new[] { 169d }, "watches/sony");

                    tsf.Append(baseline.AddHours(2).AddMinutes(1), new[] { 259d }, "watches/fitbit");
                    tsf.Append(baseline.AddHours(2).AddMinutes(2), new[] { 279d }, "watches/apple");
                    tsf.Append(baseline.AddHours(2).AddMinutes(3), new[] { 269d }, "watches/sony");

                    tsf.Append(baseline.AddMonths(1).AddMinutes(1), new[] { 359d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(2), new[] { 379d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(1).AddMinutes(3), new[] { 369d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(1), new[] { 459d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(2), new[] { 479d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(1).AddMinutes(3), new[] { 469d }, "watches/fitbit");

                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(1), new[] { 559d }, "watches/apple");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(2), new[] { 579d }, "watches/sony");
                    tsf.Append(baseline.AddMonths(6).AddHours(2).AddMinutes(3), new[] { 569d }, "watches/fitbit");


                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var offset = TimeZoneInfo.Local.BaseUtcOffset;

                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate between $start and $end
    group by '1h' 
    select min(), max()
    offset $offset
}
from People as p
select out(p)
")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(6).EnsureUtc())
                        .AddParameter("offset", offset);

                    var agg = query.First();

                    Assert.Equal(12, agg.Count);

                    Assert.Equal(4, agg.Results.Length);

                    var rangeAggregation = agg.Results[0];

                    Assert.Equal(59, rangeAggregation.Min[0]);
                    Assert.Equal(79, rangeAggregation.Max[0]);

                    var expectedFrom = baseline.Add(offset);
                    var expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[1];

                    Assert.Equal(159, rangeAggregation.Min[0]);
                    Assert.Equal(179, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[2];

                    Assert.Equal(259, rangeAggregation.Min[0]);
                    Assert.Equal(279, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddHours(2).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);

                    rangeAggregation = agg.Results[3];

                    Assert.Equal(359, rangeAggregation.Min[0]);
                    Assert.Equal(379, rangeAggregation.Max[0]);

                    expectedFrom = baseline.AddMonths(1).Add(offset);
                    expectedTo = expectedFrom.AddHours(1);

                    Assert.Equal(expectedFrom, rangeAggregation.From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(expectedTo, rangeAggregation.To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.From.Kind);
                    Assert.Equal(DateTimeKind.Unspecified, rangeAggregation.To.Kind);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnBadOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    var person = new Person
                    {
                        Name = "Oren"
                    };

                    session.Store(person);

                    var tsf = session.TimeSeriesFor(person, "HeartRate");

                    tsf.Append(baseline.AddMinutes(1), new[] { 59d }, "watches/fitbit");
                    tsf.Append(baseline.AddMinutes(2), new[] { 79d }, "watches/apple");
                    tsf.Append(baseline.AddMinutes(3), new[] { 69d }, "watches/sony");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var offset = 2;

                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    between $start and $end
    group by '1h' 
    select min(), max()
    offset $offset
}
from People as p
select out(p)")
                        .AddParameter("start", baseline.EnsureUtc())
                        .AddParameter("end", baseline.AddMonths(6).EnsureUtc())
                        .AddParameter("offset", offset);

                    var ex = Assert.Throws<RavenException>(() => query.ToList());

                    Assert.Contains($"Failed to parse object '{offset}' as TimeSpan", ex.Message);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_GroupByWithoutSelect()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");
                    var tsf = session.TimeSeriesFor("people/1", "HeartRate");

                    for (int i = 0; i < 10; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(
@"declare timeseries out(){
    from HeartRate
    where Tag != $t
    group by '2 minutes'
}
from People 
select out()
")
                        .AddParameter("t", "watches/sony");

                    var result = query.First();
                    Assert.Equal(10, result.Count);
                    Assert.Equal(5, result.Results.Length);

                    for (int i = 0; i < 10; i += 2)
                    {
                        var resultIndex = i / 2;

                        Assert.Equal(i, result.Results[resultIndex].First[0]);
                        Assert.Equal(i, result.Results[resultIndex].Min[0]);

                        Assert.Equal(i + 1, result.Results[resultIndex].Last[0]);
                        Assert.Equal(i + 1, result.Results[resultIndex].Max[0]);

                        Assert.Equal(2 * i + 1, result.Results[resultIndex].Sum[0]);
                        Assert.Equal(i + 0.5, result.Results[resultIndex].Average[0]);
                        Assert.Equal(2, result.Results[resultIndex].Count[0]);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_WithSelectSum()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person(), "people/1");
                    var tsf = session.TimeSeriesFor("people/1", "HeartRate");

                    for (int i = 0; i < 10; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(
@"declare timeseries out(){
    from HeartRate
    group by '2 minutes'
    select sum()
}
from People
select out()");
                    var result = query.First();
                    Assert.Equal(10, result.Count);
                    Assert.Equal(5, result.Results.Length);

                    for (int i = 0; i < 5; i++)
                    {
                        Assert.Equal(4 * i + 1, result.Results[i].Sum[0]);
                    }
                }
            }
        }

        [Fact(Skip = "RavenDB-14988")]
        public void CanQueryTimeSeriesRaw_UsingLast_Milliseconds()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    tsf.Append(baseline.AddMinutes(totalMinutes).AddMilliseconds(10), 100, "last");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    last 100 ms
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(2, result.Count);
                    Assert.Equal(baseline.AddMinutes(totalMinutes), result.Results[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(totalMinutes).AddMilliseconds(10), result.Results[1].Timestamp);
                    Assert.Equal("last", result.Results[1].Tag);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLast_Seconds()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    last 90 second
    group by 10 seconds
    select avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(2, result.Count);
                    Assert.Equal(2, result.Results.Length);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLast_Minutes()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    last 30 minutes
    group by 10 minutes
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(31, result.Count);
                    Assert.Equal(4, result.Results.Length);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_UsingLast_Hour()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate last 12h
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    var expectedInitialTimestamp = baseline.AddDays(3).AddHours(-12);
                    var expectedInitialValueValue = totalMinutes - TimeSpan.FromHours(12).TotalMinutes;
                    var expectedCount = totalMinutes - expectedInitialValueValue + 1;

                    Assert.Equal(expectedCount, result.Count);

                    for (int i = 0; i < expectedCount; i++)
                    {
                        Assert.Equal(expectedInitialValueValue + i, result.Results[i].Value);
                        Assert.Equal(expectedInitialTimestamp.AddMinutes(i), result.Results[i].Timestamp);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLast_Hour()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate last 12h
    group by 1h
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    var expectedInitialTimestamp = baseline.AddDays(3).AddHours(-12);
                    var expectedInitialValueValue = totalMinutes - TimeSpan.FromHours(12).TotalMinutes;
                    var expectedCount = totalMinutes - expectedInitialValueValue + 1;
                    var expectedBucketsCount = 13;

                    Assert.Equal(expectedCount, result.Count);
                    Assert.Equal(expectedBucketsCount, result.Results.Length);

                    for (int i = 0; i < expectedBucketsCount - 1; i++)
                    {
                        var expectedMin = expectedInitialValueValue + (60 * i);
                        var expectedMax = expectedMin + 59;
                        var expectedAvg = (expectedMin + expectedMax) / 2;
                        var expectedFrom = expectedInitialTimestamp.AddMinutes(i * 60);
                        var expectedTo = expectedFrom.AddHours(1);

                        Assert.Equal(expectedMin, result.Results[i].Min[0]);
                        Assert.Equal(expectedMax, result.Results[i].Max[0]);
                        Assert.Equal(expectedAvg, result.Results[i].Average[0]);
                        Assert.Equal(60, result.Results[i].Count[0]);
                        Assert.Equal(expectedFrom, result.Results[i].From);
                        Assert.Equal(expectedTo, result.Results[i].To);
                    }

                    var expected = expectedInitialValueValue + (60 * 12);
                    Assert.Equal(expected, result.Results[^1].Min[0]);
                    Assert.Equal(expected, result.Results[^1].Max[0]);
                    Assert.Equal(expected, result.Results[^1].Average[0]);
                    Assert.Equal(1, result.Results[^1].Count[0]);
                    Assert.Equal(expectedInitialTimestamp.AddHours(12), result.Results[^1].From);
                    Assert.Equal(expectedInitialTimestamp.AddHours(13), result.Results[^1].To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLast_Day()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    last 1 day
    group by 1 hour
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();


                    Assert.Equal(TimeSpan.FromDays(1).TotalMinutes + 1, result.Count);
                    Assert.Equal(TimeSpan.FromDays(1).TotalHours + 1, result.Results.Length);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLast_SelectSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People as doc
where id(doc) = $id
select timeseries(
    from doc.HeartRate 
    last 12h
    group by 1h
    select min(), max(), avg()
)")
                        .AddParameter("id", id)
                        .First();

                    var expectedInitialTimestamp = baseline.AddDays(3).AddHours(-12);
                    var expectedInitialValueValue = totalMinutes - TimeSpan.FromHours(12).TotalMinutes;
                    var expectedCount = totalMinutes - expectedInitialValueValue + 1;
                    var expectedBucketsCount = 13;

                    Assert.Equal(expectedCount, result.Count);
                    Assert.Equal(expectedBucketsCount, result.Results.Length);

                    for (int i = 0; i < expectedBucketsCount - 1; i++)
                    {
                        var expectedMin = expectedInitialValueValue + (60 * i);
                        var expectedMax = expectedMin + 59;
                        var expectedAvg = (expectedMin + expectedMax) / 2;
                        var expectedFrom = expectedInitialTimestamp.AddMinutes(i * 60);
                        var expectedTo = expectedFrom.AddHours(1);

                        Assert.Equal(expectedMin, result.Results[i].Min[0]);
                        Assert.Equal(expectedMax, result.Results[i].Max[0]);
                        Assert.Equal(expectedAvg, result.Results[i].Average[0]);
                        Assert.Equal(60, result.Results[i].Count[0]);
                        Assert.Equal(expectedFrom, result.Results[i].From);
                        Assert.Equal(expectedTo, result.Results[i].To);
                    }

                    var expected = expectedInitialValueValue + (60 * 12);
                    Assert.Equal(expected, result.Results[^1].Min[0]);
                    Assert.Equal(expected, result.Results[^1].Max[0]);
                    Assert.Equal(expected, result.Results[^1].Average[0]);
                    Assert.Equal(1, result.Results[^1].Count[0]);
                    Assert.Equal(expectedInitialTimestamp.AddHours(12), result.Results[^1].From);
                    Assert.Equal(expectedInitialTimestamp.AddHours(13), result.Results[^1].To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingLast_WithFilter()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        var tag = i % 2 == 0 ? "watches/fitbit" : "watches/apple";
                        tsf.Append(baseline.AddMinutes(i), i, tag);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate last 12h
    where tag = $tag
    group by 1h
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .AddParameter("tag", "watches/apple")
                        .First();

                    Assert.Equal(360, result.Count);
                    Assert.Equal(12, result.Results.Length);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_UsingLast_WithOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var offset = TimeZoneInfo.Local.BaseUtcOffset;

                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(
@"
declare timeseries out(doc)
{
    from doc.HeartRate
    last 12h
    offset $offset
}
from People as p
where id(p) = $id
select out(p)
")
                        .AddParameter("id", id)
                        .AddParameter("offset", offset)
                        .First();

                    var expectedInitialTimestamp = baseline.Add(offset).AddDays(3).AddHours(-12);
                    var expectedInitialValueValue = totalMinutes - TimeSpan.FromHours(12).TotalMinutes;
                    var expectedCount = totalMinutes - expectedInitialValueValue + 1;

                    Assert.Equal(expectedCount, result.Count);

                    for (int i = 0; i < expectedCount; i++)
                    {
                        Assert.Equal(expectedInitialValueValue + i, result.Results[i].Value);
                        Assert.Equal(expectedInitialTimestamp.AddMinutes(i), result.Results[i].Timestamp);
                    }
                }
            }
        }

        [Fact(Skip = "RavenDB-14988")]
        public void CanQueryTimeSeriesRaw_UsingFirst_Milliseconds()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    tsf.Append(baseline.AddMilliseconds(10), 1.5, "watches/fitbit");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    first 100 ms
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(2, result.Count);
                    Assert.Equal(baseline, result.Results[0].Timestamp);
                    Assert.Equal(baseline.AddMilliseconds(10), result.Results[1].Timestamp);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingFirst_Seconds()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    first 90 second
    group by 10 seconds
    select avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(2, result.Count);
                    Assert.Equal(2, result.Results.Length);

                    Assert.Equal(baseline, result.Results[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(result.Results[0].From.AddSeconds(10), result.Results[0].To, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(1), result.Results[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(result.Results[1].From.AddSeconds(10), result.Results[1].To, RavenTestHelper.DateTimeComparer.Instance);

                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingFirst_Minutes()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    first 30 minutes
    group by 10 minutes
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(31, result.Count);
                    Assert.Equal(4, result.Results.Length);

                    Assert.Equal(baseline, result.Results[0].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(10), result.Results[1].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(20), result.Results[2].From, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal(baseline.AddMinutes(30), result.Results[3].From, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_UsingFirst_Hour()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate first 12h
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    var expectedCount = TimeSpan.FromHours(12).TotalMinutes + 1;

                    Assert.Equal(expectedCount, result.Count);

                    for (int i = 0; i < expectedCount; i++)
                    {
                        Assert.Equal(i, result.Results[i].Value);
                        Assert.Equal(baseline.AddMinutes(i), result.Results[i].Timestamp);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingFirst_Hour()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate first 12h
    group by 1h
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    var expectedCount = TimeSpan.FromHours(12).TotalMinutes + 1;
                    var expectedBucketsCount = 13;

                    Assert.Equal(expectedCount, result.Count);
                    Assert.Equal(expectedBucketsCount, result.Results.Length);

                    for (int i = 0; i < expectedBucketsCount - 1; i++)
                    {
                        var expectedMin =  60 * i;
                        var expectedMax = expectedMin + 59;
                        var expectedAvg = (expectedMin + expectedMax) / 2.0;
                        var expectedFrom = baseline.AddMinutes(i * 60);
                        var expectedTo = expectedFrom.AddHours(1);

                        Assert.Equal(expectedMin, result.Results[i].Min[0]);
                        Assert.Equal(expectedMax, result.Results[i].Max[0]);
                        Assert.Equal(expectedAvg, result.Results[i].Average[0]);
                        Assert.Equal(60, result.Results[i].Count[0]);
                        Assert.Equal(expectedFrom, result.Results[i].From);
                        Assert.Equal(expectedTo, result.Results[i].To);
                    }

                    var expected = 60 * 12;
                    Assert.Equal(expected, result.Results[^1].Min[0]);
                    Assert.Equal(expected, result.Results[^1].Max[0]);
                    Assert.Equal(expected, result.Results[^1].Average[0]);
                    Assert.Equal(1, result.Results[^1].Count[0]);
                    Assert.Equal(baseline.AddHours(12), result.Results[^1].From);
                    Assert.Equal(baseline.AddHours(13), result.Results[^1].To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingFirst_Day()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    first 1 day
    group by 1 hour
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .First();

                    Assert.Equal(TimeSpan.FromDays(1).TotalMinutes + 1, result.Count);
                    Assert.Equal(TimeSpan.FromDays(1).TotalHours + 1, result.Results.Length);

                    for (var i = 0; i < result.Results.Length; i++)
                    {
                        var range = result.Results[i];
                        Assert.Equal(baseline.AddHours(i), range.From, RavenTestHelper.DateTimeComparer.Instance);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingFirst_SelectSyntax()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
from People as doc
where id(doc) = $id
select timeseries(
    from doc.HeartRate 
    first 12h
    group by 1h
    select min(), max(), avg()
)")
                        .AddParameter("id", id)
                        .First();

                    var expectedCount = TimeSpan.FromHours(12).TotalMinutes + 1;
                    var expectedBucketsCount = 13;

                    Assert.Equal(expectedCount, result.Count);
                    Assert.Equal(expectedBucketsCount, result.Results.Length);

                    for (int i = 0; i < expectedBucketsCount - 1; i++)
                    {
                        var expectedMin = 60 * i;
                        var expectedMax = expectedMin + 59;
                        var expectedAvg = (expectedMin + expectedMax) / 2.0;
                        var expectedFrom = baseline.AddMinutes(i * 60);
                        var expectedTo = expectedFrom.AddHours(1);

                        Assert.Equal(expectedMin, result.Results[i].Min[0]);
                        Assert.Equal(expectedMax, result.Results[i].Max[0]);
                        Assert.Equal(expectedAvg, result.Results[i].Average[0]);
                        Assert.Equal(60, result.Results[i].Count[0]);
                        Assert.Equal(expectedFrom, result.Results[i].From);
                        Assert.Equal(expectedTo, result.Results[i].To);
                    }

                    var expected = 60 * 12;
                    Assert.Equal(expected, result.Results[^1].Min[0]);
                    Assert.Equal(expected, result.Results[^1].Max[0]);
                    Assert.Equal(expected, result.Results[^1].Average[0]);
                    Assert.Equal(1, result.Results[^1].Count[0]);
                    Assert.Equal(baseline.AddHours(12), result.Results[^1].From);
                    Assert.Equal(baseline.AddHours(13), result.Results[^1].To);
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesAggregation_UsingFirst_WithFilter()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        var tag = i % 2 == 0 ? "watches/fitbit" : "watches/apple";
                        tsf.Append(baseline.AddMinutes(i), i, tag);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate 
    FIRST 12h
    where tag = $tag
    group by 1h
    select min(), max(), avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .AddParameter("tag", "watches/apple")
                        .First();

                    Assert.Equal(360, result.Count);
                    Assert.Equal(12, result.Results.Length);

                    for (int i = 0; i < result.Results.Length; i++)
                    {
                        Assert.Equal(baseline.AddHours(i), result.Results[i].From);
                    }
                }
            }
        }

        [Fact]
        public void CanQueryTimeSeriesRaw_UsingFirst_WithOffset()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int i = 0; i <= totalMinutes; i++)
                    {
                        tsf.Append(baseline.AddMinutes(i), i, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var offset = TimeZoneInfo.Local.BaseUtcOffset;

                    var result = session.Advanced.RawQuery<TimeSeriesRawResult>(
@"
declare timeseries out(doc)
{
    from doc.HeartRate
    first 12h
    offset $offset
}
from People as p
where id(p) = $id
select out(p)
")
                        .AddParameter("id", id)
                        .AddParameter("offset", offset)
                        .First();

                    var expectedInitialTimestamp = baseline.Add(offset);
                    var expectedCount = TimeSpan.FromHours(12).TotalMinutes + 1;

                    Assert.Equal(expectedCount, result.Count);

                    for (int i = 0; i < expectedCount; i++)
                    {
                        Assert.Equal(i, result.Results[i].Value);
                        Assert.Equal(expectedInitialTimestamp.AddMinutes(i), result.Results[i].Timestamp);
                    }
                }
            }
        }

        [Fact]
        public void ShouldThrowOnUsingLastAndBetweenInTheSameTimeSeriesQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    session.TimeSeriesFor(id, "HeartRate").Append(baseline, 60, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate
    between $stat and $end
    last 6 hours
    group by 1 hour
    select avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));


                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());
                    Assert.Contains("Cannot have both 'Last' and 'Between' in the same Time Series query function", ex.Message);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnUsingFirstAndBetweenInTheSameTimeSeriesQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    session.TimeSeriesFor(id, "HeartRate").Append(baseline, 60, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate
    between $stat and $end
    first 6 hours
    group by 1 hour
    select avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));


                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());
                    Assert.Contains("Cannot have both 'First' and 'Between' in the same Time Series query function", ex.Message);
                }
            }
        }

        [Fact]
        public void ShouldThrowOnUsingFirstAndLastInTheSameTimeSeriesQuery()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday.EnsureUtc().AddDays(-7);
                var id = "people/1";
                var totalMinutes = TimeSpan.FromDays(3).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Oren",
                    }, id);

                    session.TimeSeriesFor(id, "HeartRate").Append(baseline, 60, "watches/fitbit");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult>(@"
declare timeseries out(x) 
{
    from x.HeartRate
    first 6 hours
    last 12 minutes
    group by 1 hour
    select avg()
}
from People as doc
where id(doc) = $id
select out(doc)
")
                        .AddParameter("id", id)
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddDays(1));


                    var ex = Assert.Throws<InvalidQueryException>(() => query.ToList());
                    Assert.Contains("Cannot have both 'First' and 'Last' in the same Time Series query function", ex.Message);
                }
            }
        }

        [Fact]
        public async Task CanQueryTimeSeriesUsingNamedValues()
        {
            using (var store = GetDocumentStore())
            {
                await store.TimeSeries.RegisterAsync<Company, StockPrice>();

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;

                var stock = updated.GetNames("companies", nameof(StockPrice) + "s");
                Assert.Equal(5, stock.Length);
                Assert.Equal(nameof(StockPrice.Open), stock[0]);
                Assert.Equal(nameof(StockPrice.Close), stock[1]);
                Assert.Equal(nameof(StockPrice.High), stock[2]);
                Assert.Equal(nameof(StockPrice.Low), stock[3]);
                Assert.Equal(nameof(StockPrice.Volume), stock[4]);

                var baseline = RavenTestHelper.UtcToday;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    var tsf = session.TimeSeriesFor<StockPrice>("companies/1", "StockPrices");
                    var random = new Random();
                    for (int i = 0; i < 10; i++)
                    {
                        var rand = random.Next(1, 10) / 10.0;
                        var even = i % 2 == 0;
                        var add = even ? rand : -rand;

                        tsf.Append(baseline.AddHours(i), new StockPrice
                        {
                            Open = 45.37 + add,
                            Close = 45.72 + add,
                            High = 45.99 + add,
                            Low = 45.21 + add,
                            Volume = 719.636 + add
                        }, tag: even ? "tags/1" : "tags/2");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesRawResult<StockPrice>>(@"
declare timeseries out(c){
    from c.StockPrices 
    between $start and $end
    where High > 45.99
}
from Companies as c 
where id() == 'companies/1'
select out(c)
")
                        .AddParameter("start", baseline.AddDays(-1).EnsureUtc())
                        .AddParameter("end", baseline.AddDays(1).EnsureUtc());

                    var result = query.First();

                    Assert.Equal(5, result.Count);
                    foreach (var entry in result.Results)
                    {
                        Assert.True(entry.Value.High > 45.99);
                        Assert.Equal(entry.Tag, "tags/1");
                    }
                }
            }
        }


        [Fact]
        public async Task CanQueryTimeSeriesAggregationUsingNamedValues()
        {
            const string seriesName = "StockPrices";
            using (var store = GetDocumentStore())
            {
                await store.TimeSeries.RegisterAsync<Company, StockPrice>();

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;

                var stock = updated.GetNames("companies", seriesName);
                Assert.Equal(5, stock.Length);
                Assert.Equal(nameof(StockPrice.Open), stock[0]);
                Assert.Equal(nameof(StockPrice.Close), stock[1]);
                Assert.Equal(nameof(StockPrice.High), stock[2]);
                Assert.Equal(nameof(StockPrice.Low), stock[3]);
                Assert.Equal(nameof(StockPrice.Volume), stock[4]);

                var baseline = DateTime.Today;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    var tsf = session.TimeSeriesFor<StockPrice>("companies/1", seriesName);
                    var random = new Random();
                    for (int i = 0; i < 70; i++)
                    {
                        var rand = random.Next(1, 10) / 10.0;
                        var add = i % 7 != 0 ? rand : -rand;

                        tsf.Append(baseline.AddDays(i), new StockPrice
                        {
                            Open = 45.37 + add,
                            Close = 45.72 + add,
                            High = 45.99 + add,
                            Low = 45.21 + add,
                            Volume = 719.636 + add
                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<TimeSeriesAggregationResult<StockPrice>>(@"
from Companies
where id() == 'companies/1'
select timeseries(
    from StockPrices 
    between $start and $end
    where High > 45.99
    group by '7 days'
    select max())
")
                        .AddParameter("start", baseline)
                        .AddParameter("end", baseline.AddMonths(3));

                    var result = query.First();

                    Assert.Equal(60, result.Count);
                    Assert.Equal(10, result.Results.Length);
                    foreach (var entry in result.Results)
                    {
                        Assert.True(entry.Max.High > 45.99);
                        Assert.Equal(6, entry.Count.High);
                    }
                }
            }
        }
    }
}
