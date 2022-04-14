using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18399 : RavenTestBase
{
    // No nullable TimeOnly & DateOnly tests here: RavenDB_17250

    public RavenDB_18399(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void DateAndTimeOnlyTestInIndex()
    {
        using var store = GetDocumentStore();
        CreateDatabaseData(store);
        CreateIndex<DateAndTimeOnlyIndex>(store);
        {
            var @do = DateOnly.MaxValue;
            using var session = store.OpenSession();

            var resultRaw2 = session
                .Query<DateAndTimeOnlyIndex.IndexEntry, DateAndTimeOnlyIndex>()
                .Where(p => p.DateOnly < @do).OrderBy(p => p.DateOnly)
                .ProjectInto<DateAndTimeOnly>();

            var result = resultRaw2.ToList();
            result.ForEach(i => Assert.True(i.DateOnly < @do));
        }
    }

    [Fact]
    public void IndexWithLetQueries()
    {
        using var store = GetDocumentStore();
        CreateDatabaseData(store);
        CreateIndex<MapReduceWithLetAndNullableItems>(store);
        using var session = store.OpenSession();
        var result = session
            .Query<MapReduceWithLetAndNullableItems.IndexEntry, MapReduceWithLetAndNullableItems>()
            .Where(p => p.Year == 2)
            .ProjectInto<DateAndTimeOnly>()
            .ToList();

        result.ForEach(i =>
        {
            Assert.Equal(2, i.DateOnly?.Year);
        });
    }

    [Fact]
    public void UsingFilter()
    {
        var dateOnly = default(DateOnly).AddDays(300);
        var timeOnly = new TimeOnly(0, 0, 0, 234).AddMinutes(300);

        using var store = GetDocumentStore();
        CreateDatabaseData(store);
        using var session = store.OpenSession();
        var result = session
            .Query<DateAndTimeOnly>()
            .Filter(p => p.DateOnly == dateOnly)
            .Single();
        Assert.Equal(dateOnly, result.DateOnly);
        result = session
            .Query<DateAndTimeOnly>()
            .Filter(p => p.TimeOnly == timeOnly)
            .Single();
        Assert.Equal(timeOnly, result.TimeOnly);
    }

    [Fact]
    public void DateTimeToDateOnlyWithLet()
    {
        using var store = GetDocumentStore();
        CreateDatabaseData(store);
        CreateIndex<IndexWithDateTimeAndDateOnly>(store);
        
        using var session = store.OpenSession();

        var result = session
            .Query<IndexWithDateTimeAndDateOnly.IndexEntry, IndexWithDateTimeAndDateOnly>()
            .Where(p => p.Year == 2)
            .ProjectInto<DateAndTimeOnly>()
            .ToList();
        
        result.ForEach(i =>
        {
            Assert.Equal(2, i.DateOnly?.Year);
        });
    }

    [Fact]
    public void TransformDateInJsPatch()
    {
        using var store = GetDocumentStore();
        var @do = new DateOnly(2022, 2, 21);
        var to = new TimeOnly(21, 11, 00);
        var entity = new DateAndTimeOnly() {DateOnly = @do, TimeOnly = to};
        {
            using var session = store.OpenSession();
            session.Store(entity);
            session.Store(new DateAndTimeOnly());
            session.SaveChanges();
        }
        var operation = store.Operations.Send(new PatchByQueryOperation(@"
declare function modifyDateInJs(date, days) {
  var result = new Date(date);
  result.setDate(result.getDate() + days);
  return result.toISOString().substring(0,10);
}

from DateAndTimeOnlies where DateOnly != null update { this.DateOnly = modifyDateInJs(this.DateOnly, 1); }"));
        operation.WaitForCompletion(TimeSpan.FromSeconds(5));
        {
            using var session = store.OpenSession();
            WaitForUserToContinueTheTest(store);
            var result = session.Query<DateAndTimeOnly>().ToList();
            Assert.Null(result.Single(p => p.DateOnly == null).DateOnly);
            Assert.Equal(@do.AddDays(1), result.Single(p=> p.DateOnly != null).DateOnly);
        }
    }


    [Fact]
    public void PatchDateOnlyAndTimeOnly()
    {
        using var store = GetDocumentStore();
        var @do = new DateOnly(2022, 2, 21);
        var to = new TimeOnly(21, 11, 00);
        string id;
        var entity = new DateAndTimeOnly()
        {
            DateOnly = @do, 
            TimeOnly = to
        };
        {
            using var session = store.OpenSession();
            session.Store(entity);
            session.SaveChanges();
            id = session.Advanced.GetDocumentId(entity);
        }

        {
            using var session = store.OpenSession();
            session.Advanced.Patch<DateAndTimeOnly, DateOnly>(id, x => x.DateOnly.Value, @do.AddDays(1));
            session.SaveChanges();
        }

        {
            using var session = store.OpenSession();
            var single = session.Query<DateAndTimeOnly>().Single();
            Assert.Equal(@do.AddDays(1), single.DateOnly);
        }

        {
            using var session = store.OpenSession();
            session.Advanced.Patch<DateAndTimeOnly, TimeOnly>(id, x => x.TimeOnly.Value, to.AddHours(1));
            session.SaveChanges();
        }

        {
            using var session = store.OpenSession();
            var single = session.Query<DateAndTimeOnly>().Single();
            Assert.Equal(to.AddHours(1), single.TimeOnly);
        }
    }


    [Fact]
    public void DateAndTimeOnlyInQuery()
    {
        using var store = GetDocumentStore();

        var data = CreateDatabaseData(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var date = default(DateOnly).AddDays(500);
            var time = default(TimeOnly);
            var resultRawQuery = session.Query<DateAndTimeOnly>().Where(p => p.DateOnly >= date && p.TimeOnly > time);
            var result = resultRawQuery.ToList();
            WaitForUserToContinueTheTest(store);
            Assert.Equal(500, result.Count);
            var definitions = store.Maintenance.Send(new GetIndexesOperation(0, 1));
            foreach (var indexDefinition in definitions)
            {
                foreach (string fieldsKey in indexDefinition.Fields.Keys)
                {
                    Assert.False(fieldsKey.Contains("_Time"));
                }
            }

            result.ForEach(i => Assert.True(i.DateOnly >= date && i.TimeOnly > time));

            Assert.Equal(1, session.Query<DateAndTimeOnly>().Count(p => p.DateOnly == null));
            Assert.Equal(1, session.Query<DateAndTimeOnly>().Count(p => p.TimeOnly == null));
            var nullResult = session.Query<DateAndTimeOnly>().Single(p => p.DateOnly == null);
            Assert.Null(nullResult.Age);
            Assert.Null(nullResult.DateOnly);
            Assert.Null(nullResult.TimeOnly);
            Assert.Null(nullResult.DateTime);
        }
    }

    [Fact]
    public void QueriesAsString()
    {
        using var store = GetDocumentStore();
        var data = CreateDatabaseData(store);

        {
            using var session = store.OpenSession();
            var after = new TimeOnly(15, 00);
            var before = new TimeOnly(17, 00);
            var result = session.Query<DateAndTimeOnly>()
                .Where(i => i.TimeOnly > after && i.TimeOnly < before)
                .OrderBy(p => p.TimeOnly)
                .ToList();

            var testData = data
                .Where(i => i.TimeOnly > after && i.TimeOnly < before)
                .OrderBy(p => p.TimeOnly)
                .ToList();

            Assert.True(
                testData
                    .Select(p => p.TimeOnly)
                    .SequenceEqual(
                        result
                            .Select(p => p.TimeOnly)
                    )
            );
        }

        {
            using var session = store.OpenSession();
            var after = new DateOnly(1, 9, 1);
            var before = new DateOnly(2, 6, 17);
            var result = session.Query<DateAndTimeOnly>()
                .Where(i => i.DateOnly > after && i.DateOnly < before)
                .OrderBy(i => i.DateOnly)
                .ToList();

            var testData = data
                .Where(i => i.DateOnly > after && i.DateOnly < before)
                .OrderBy(i => i.DateOnly)
                .ToList();


            Assert.True(
                testData
                    .Select(p => p.DateOnly)
                    .SequenceEqual(
                        result
                            .Select(p => p.DateOnly)
                    )
            );
        }
    }

    [Fact]
    public void QueriesAsTicks()
    {
        using var store = GetDocumentStore();
        var data = CreateDatabaseData(store);
        CreateIndex<DateAndTimeOnlyIndex>(store);

        {
            using var session = store.OpenSession();
            var after = new TimeOnly(15, 00);
            var before = new TimeOnly(17, 00);
            var result = session.Query<DateAndTimeOnly, DateAndTimeOnlyIndex>()
                .Where(i => i.TimeOnly > after && i.TimeOnly < before)
                .OrderBy(p => p.TimeOnly)
                .ToList();

            var testData = data
                .Where(i => i.TimeOnly > after && i.TimeOnly < before)
                .OrderBy(p => p.TimeOnly)
                .ToList();

            Assert.True(
                testData
                    .Select(p => p.TimeOnly)
                    .SequenceEqual(
                        result
                            .Select(p => p.TimeOnly)
                    )
            );
        }

        {
            using var session = store.OpenSession();
            var after = new DateOnly(1, 9, 1);
            var before = new DateOnly(2, 6, 17);
            var result = session.Query<DateAndTimeOnly, DateAndTimeOnlyIndex>()
                .Where(i => i.DateOnly > after && i.DateOnly < before)
                .OrderBy(i => i.DateOnly)
                .ToList();

            var testData = data
                .Where(i => i.DateOnly > after && i.DateOnly < before)
                .OrderBy(i => i.DateOnly)
                .ToList();


            Assert.True(
                testData
                    .Select(p => p.DateOnly)
                    .SequenceEqual(
                        result
                            .Select(p => p.DateOnly)
                    )
            );
        }
    }

    [Fact]
    public void MinMaxValueInProjections()
    {
        using var store = GetDocumentStore();
        {
            using var session = store.OpenSession();
            session.Store(new ProjectionTestWithDefaultValues {Min = DateOnly.MaxValue, Max = DateOnly.MinValue, Time = DateTime.Today}
            );
            session.Store(new ProjectionTestWithDefaultValues {Min = null, Max = null, Time = null});
            session.SaveChanges();
        }
        {
            using var session = store.OpenSession();
            var result = session.Query<ProjectionTestWithDefaultValues>().Where(w => w.Max == DateOnly.MinValue)
                .Select(p => new ProjectionTestWithDefaultValues {Min = DateOnly.MinValue, Max = DateOnly.MaxValue}).Single();
            Assert.Equal(DateOnly.MinValue, result.Min);
            Assert.Equal(DateOnly.MaxValue, result.Max);
        }
    }

    private class ProjectionTestWithDefaultValues
    {
        public DateOnly? Min { get; set; }
        public DateOnly? Max { get; set; }

        public DateTime? Time { get; set; }
    }

    [Fact]
    public void ProjectionJobsWithDateTimeDateOnly()
    {
        using var store = GetDocumentStore();
        {
            using var s = store.OpenSession();
            s.Store(new DateAndTimeOnly() {TimeOnly = TimeOnly.MaxValue, DateOnly = new DateOnly(1947, 12, 21)});
            s.Store(new DateAndTimeOnly());

            s.SaveChanges();
        }
        var today = DateOnly.FromDateTime(DateTime.Today);
        {
            using var s = store.OpenSession();
            var q = s
                .Query<DateAndTimeOnly>()
                .Where(p => p.DateOnly != null)
                .Select(p => new DateAndTimeOnly {Age = (today.Year - p.DateOnly.Value.Year)})
                .Single();
            Assert.Equal(today.Year - 1947, q.Age);
        }
    }

    private List<DateAndTimeOnly> CreateDatabaseData(IDocumentStore store)
    {
        TimeOnly timeOnly = new TimeOnly(0, 0, 0, 234);
        DateOnly dateOnly;
        var database =
            Enumerable.Range(0, 1000)
                .Select(i => new DateAndTimeOnly() {TimeOnly = timeOnly.AddMinutes(i), DateOnly = dateOnly.AddDays(i), DateTime = DateTime.Now}).ToList();

        database.Add(
            new DateAndTimeOnly() {Age = null, DateOnly = null, DateTime = null, TimeOnly = null});

        {
            using var bulkInsert = store.BulkInsert();
            database.ForEach(i => bulkInsert.Store(i));
        }
        return database;
    }

    private TIndex CreateIndex<TIndex>(IDocumentStore store)
        where TIndex : AbstractIndexCreationTask, new()
    {
        var index = new TIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] {index.IndexName}));
        WaitForUserToContinueTheTest(store);
        Assert.Equal(0, indexErrors[0].Errors.Length);
        return index;
    }

    private class DateAndTimeOnly
    {
        public DateOnly? DateOnly { get; set; }
        public TimeOnly? TimeOnly { get; set; }
        public DateTime? DateTime { get; set; }

        public int? Age { get; set; }
    }

    private class DateAndTimeOnlyIndex : AbstractIndexCreationTask<DateAndTimeOnly, DateAndTimeOnlyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public DateOnly? DateOnly { get; set; }
            public int Year { get; set; }

            public TimeOnly? TimeOnly { get; set; }
        }

        public DateAndTimeOnlyIndex()
        {
            Map = dates => from date in dates
                select new IndexEntry
                {
                    DateOnly = date.DateOnly,
                    TimeOnly = date.TimeOnly
                };
        }
    }

    private class MapReduceWithLetAndNullableItems : AbstractIndexCreationTask<DateAndTimeOnly, DateAndTimeOnlyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public DateOnly? DateOnly { get; set; }
            public int Year { get; set; }
            public TimeOnly? TimeOnly { get; set; }
        }

        public MapReduceWithLetAndNullableItems()
        {
            Map = dates => from date in dates
                let x = date.DateOnly.Value
                select new IndexEntry {Year = x.Year, DateOnly = x, TimeOnly = date.TimeOnly};

            Reduce = entries => from entry in entries
                group entry by entry.DateOnly
                into g
                select new IndexEntry {DateOnly = g.Key, Year = g.Sum(x => x.Year), TimeOnly = null};
        }
    }

    #region IndexWithDateTimeAndDateOnly

    private class IndexWithDateTimeAndDateOnly : AbstractIndexCreationTask<DateAndTimeOnly, DateAndTimeOnlyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public DateOnly? DateOnly { get; set; }
            public int Year { get; set; }
            public DateTime? DateTime { get; set; }
        }

        public IndexWithDateTimeAndDateOnly()
        {
            Map = dates => from date in dates
                let x = date.DateTime
                select new IndexEntry()
                {
                    Year = x.Value.Year, 
                    DateOnly = x == null ? null : DateOnly.FromDateTime(x.Value),
                    DateTime = x.Value
                };
        }
    }

    #endregion
}
