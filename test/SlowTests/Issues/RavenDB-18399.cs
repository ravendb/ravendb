using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
        var data = CreateDatabaseData(store);
        var index = new DateAndTimeOnlyIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        {
            var @do = DateOnly.MaxValue;
            var ts = @do.ToString("O", CultureInfo.InvariantCulture);
            using var session = store.OpenSession();

            var resultRaw2 = session.Query<DateAndTimeOnlyIndex.IndexEntry, DateAndTimeOnlyIndex>().Where(p => p.DateOnly < @do).OrderBy(p => p.DateOnly)
                .ProjectInto<DateAndTimeOnly>();
            var result = resultRaw2.ToList();
            result.ForEach(i => Assert.True(i.DateOnly < @do));
            WaitForUserToContinueTheTest(store);
        }
    }

    [Fact]
    public void IndexWithLetQueries()
    {
        using var store = GetDocumentStore();
        var data = CreateDatabaseData(store);
        var index = new IndexWithLet();
        index.Execute(store);
        WaitForUserToContinueTheTest(store);
        // Indexes.WaitForIndexing(store);
        {
            var @do = DateOnly.MaxValue;
            var ts = @do.ToString("O", CultureInfo.InvariantCulture);
            using var session = store.OpenSession();

            var resultRaw2 = session.Query<IndexWithLet.IndexEntry, IndexWithLet>().Where(p => p.Year == 5).ProjectInto<DateAndTimeOnly>();
            var result = resultRaw2.ToList();
            result.ForEach(i =>
            {
                Assert.Equal(5, i.DateOnly?.Year);
            });
        }
    }

    [Fact]
    public void DateTimeToDateOnlyWithLet()
    {
        using var store = GetDocumentStore();
        var data = CreateDatabaseData(store);
        var index = new IndexWithDateTimeAndDateOnly();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        {
            var @do = DateOnly.MaxValue;
            var ts = @do.ToString("O", CultureInfo.InvariantCulture);
            using var session = store.OpenSession();

            var resultRaw2 = session.Query<IndexWithDateTimeAndDateOnly.IndexEntry, IndexWithDateTimeAndDateOnly>().Where(p => p.Year == 5)
                .ProjectInto<DateAndTimeOnly>();
            var result = resultRaw2.ToList();
            result.ForEach(i =>
            {
                Assert.Equal(5, i.DateOnly?.Year);
            });
            WaitForUserToContinueTheTest(store);
        }
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
            session.SaveChanges();
        }
        var operation = store.Operations.Send(new PatchByQueryOperation(@"
declare function modifyDateInJs(date, days) {
  var result = new Date(date);
  result.setDate(result.getDate() + days);
  return result.toISOString().substring(0,10);
}

from DateAndTimeOnlies update { this.DateOnly = modifyDateInJs(this.DateOnly, 1); }"));
        operation.WaitForCompletion(TimeSpan.FromSeconds(5));
        {
            using var session = store.OpenSession();
            var single = session.Query<DateAndTimeOnly>().Single();
            Assert.Equal(@do.AddDays(1), single.DateOnly);
        }
    }


    [Fact]
    public void PatchDateOnlyAndTimeOnly()
    {
        using var store = GetDocumentStore();
        var @do = new DateOnly(2022, 2, 21);
        var to = new TimeOnly(21, 11, 00);
        string id;
        var entity = new DateAndTimeOnly() {DateOnly = @do, TimeOnly = to};
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
    public void Queries()
    {
        using var store = GetDocumentStore();
        var data = CreateDatabaseData(store);

        {
            using var session = store.OpenSession();
            var after = new TimeOnly(15, 00);
            var before = new TimeOnly(17, 00);
            var result = session.Query<DateAndTimeOnly>().Where(i => i.TimeOnly > after && i.TimeOnly < before).ToList();
            result.ForEach(i =>
            {
                Assert.True(i.TimeOnly > after && i.TimeOnly < before);
            });
        }

        {
            using var session = store.OpenSession();
            var after = new DateOnly(1, 9, 1);
            var before = new DateOnly(2, 6, 17);
            var result = session.Query<DateAndTimeOnly>().Where(i => i.DateOnly > after && i.DateOnly < before).ToList();
            result.ForEach(i =>
            {
                Assert.True(i.DateOnly > after && i.DateOnly < before);
            });
        }
    }

    [Fact]
    public void MinMaxValueInProjections()
    {
        using var store = GetDocumentStore();
        {
            using var session = store.OpenSession();
            session.Store(new ProjectionTestWithDefaultValues {Min = DateOnly.MaxValue, Max = DateOnly.MinValue}
            );
            session.SaveChanges();
        }
        {
            using var session = store.OpenSession();
            var result = session.Query<ProjectionTestWithDefaultValues>().Select(p => new ProjectionTestWithDefaultValues
            {
                Min = DateOnly.MinValue, Max = DateOnly.MaxValue
            }).Single();
            Assert.Equal(DateOnly.MinValue, result.Min);
            Assert.Equal(DateOnly.MaxValue, result.Max);
        }
    }

    private class ProjectionTestWithDefaultValues
    {
        public DateOnly Min { get; set; }
        public DateOnly Max { get; set; }
    }

    [Fact]
    public void ProjectionJobsWithDateTimeDateOnly()
    {
        using var store = GetDocumentStore();
        {
            using var s = store.OpenSession();
            s.Store(new DateAndTimeOnly() {DateOnly = new DateOnly(1947, 12, 21)});
            s.SaveChanges();
        }
        var today = DateOnly.FromDateTime(DateTime.Today);
        {
            using var s = store.OpenSession();
            var q = s.Query<DateAndTimeOnly>().Select(p => new DateAndTimeOnly() {Age =  (today.Year - p.DateOnly.Value.Year)}).Single();
            Assert.Equal(today.Year - 1947, q.Age);
        }
    }

    private List<DateAndTimeOnly> CreateDatabaseData(IDocumentStore store)
    {
        TimeOnly timeOnly = new TimeOnly(0, 0, 0, 234);
        DateOnly dateOnly;
        var database = Enumerable.Range(0, 1000).Select(
            i => new DateAndTimeOnly() {TimeOnly = timeOnly.AddMinutes(i), DateOnly = dateOnly.AddDays(i), DateTime = DateTime.Now}).ToList();
        database.Add(new DateAndTimeOnly() {Age = null, DateOnly = null, DateTime = null, TimeOnly = null});
        using var bulkInsert = store.BulkInsert();
        database.ForEach(i => bulkInsert.Store(i));
        return database;
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

            public DateOnly? DateOnlyString { get; set; }

            public TimeOnly? TimeOnlyString { get; set; }
            public TimeOnly? TimeOnly { get; set; }
        }

        public DateAndTimeOnlyIndex()
        {
            Map = dates => from date in dates
                select new IndexEntry() {DateOnly = date.DateOnly, TimeOnly = date.TimeOnly};
        }
    }

    private class IndexWithLet : AbstractIndexCreationTask<DateAndTimeOnly, DateAndTimeOnlyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public DateOnly? DateOnly { get; set; }
            public int Year { get; set; }
            public TimeOnly? TimeOnly { get; set; }
        }

        public IndexWithLet()
        {
            Map = dates => from date in dates
                let x = date.DateOnly ?? default
                select new IndexEntry() {Year = x.Year, DateOnly = new DateOnly(2020, 12, 24), TimeOnly = date.TimeOnly};
        }
    }

    private class IndexWithDateTimeAndDateOnly : AbstractIndexCreationTask<DateAndTimeOnly, DateAndTimeOnlyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public DateOnly DateOnly { get; set; }
            public int Year { get; set; }
            public DateTime DateTime { get; set; }
        }

        public IndexWithDateTimeAndDateOnly()
        {
            Map = dates => from date in dates
                let x = date.DateTime
                select new IndexEntry() {Year = x.Value.Year, DateOnly = DateOnly.FromDateTime(x.Value), DateTime = x.Value};
        }
    }
}
