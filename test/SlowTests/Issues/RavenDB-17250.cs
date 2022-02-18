using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Client.TimeSeries.BulkInsert;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17250 : RavenTestBase
{
    public RavenDB_17250(ITestOutputHelper output) : base(output)
    {
    }


    [Fact]
    public void DateAndTimeOnlyTestInIndex()
    {
        using var store = GetDocumentStore();
        var data = CreateDatabaseData(store);
        var index = new DateAndTimeOnlyIndex();
        index.Execute(store);
        WaitForIndexing(store);
        {
            var @do = DateOnly.MaxValue;
            var ts = @do.ToString("O", CultureInfo.InvariantCulture);
            using var session = store.OpenSession();

            var resultRaw2 = session.Query<DateAndTimeOnlyIndex.IndexEntry, DateAndTimeOnlyIndex>().Where(p => p.DateOnly < @do).OrderBy(p => p.DateOnly).ProjectInto<DateAndTimeOnly>();
            var result = resultRaw2.ToList();
            result.ForEach(i => Assert.True(i.DateOnly < @do));
            WaitForUserToContinueTheTest(store);
        }
    }

    [Fact]
    public void DateOnlyTimeOnlyIndexTransformationFromDiffrentsTypes()
    {
        using var store = GetDocumentStore();
        var data = CreateDatabaseData(store);
        var index = new DateAndTimeOnlyIndexWithDateTime();
        index.Execute(store);
        WaitForIndexing(store);
        {
            var @do = DateOnly.MaxValue;
            var ts = @do.ToString("O", CultureInfo.InvariantCulture);
            using var session = store.OpenSession();

            var resultRaw2 = session.Query<DateAndTimeOnlyIndexWithDateTime.IndexEntry, DateAndTimeOnlyIndexWithDateTime>().Where(p => p.DateOnly < @do).OrderBy(p => p.DateOnly).ProjectInto<DateAndTimeOnly>();
            var result = resultRaw2.ToList();
            result.ForEach(i => Assert.True(i.DateOnly < @do));
        }
    }

    private class DateTimeTicks
    {
        public DateTime BenchTicks { get; set; }
    }

    [Fact]
    public void DateAndTimeOnlyInQueryPerfTest()
    {
        int items = 100_000, queries = 10_000;
        using var store = GetDocumentStore();
        var rnd = new Random(124323567);
        {
            using var bulkInsert = store.BulkInsert();
            var list = Enumerable.Range(0, items).Select(i =>
                new DateTimeTicks() { BenchTicks = new DateTime(rnd.NextInt64(DateTime.MinValue.Ticks + 1, DateTime.MaxValue.Ticks - 1)) });
            foreach (var dateTime in list)
                bulkInsert.Store(dateTime);
        }
        foreach (int i in Enumerable.Range(0, queries))
        {
            using var session = store.OpenSession();
            var dt = new DateTime(rnd.NextInt64(DateTime.MinValue.Ticks + 1, DateTime.MaxValue.Ticks - 1));
            var t = session.Advanced.RawQuery<DateTimeTicks>("from DateTimeTicks where BenchTicks < $p0").AddParameter("p0", dt.ToString("O").Substring(0, 10))
                .ToList();
        }
    }


    [Fact]
    public void DateAndTimeOnlyInQuery()
    {
        using var store = GetDocumentStore();

        var data = CreateDatabaseData(store);
        WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            var date = default(DateOnly).AddDays(500);
            var time = default(TimeOnly);
            var resultRawQuery = session.Query<DateAndTimeOnly>().Where(p => p.DateOnly >= date && p.TimeOnly > time);
            var result = resultRawQuery.ToList();
            Assert.Equal(500, result.Count);
            WaitForUserToContinueTheTest(store);
            var definitions = store.Maintenance.Send(new GetIndexesOperation(0, 1));
            foreach (var indexDefinition in definitions)
            {
                foreach (string fieldsKey in indexDefinition.Fields.Keys)
                {
                    Assert.False(fieldsKey.Contains("_Time"));
                }
            }

            result.ForEach(i => Assert.True(i.DateOnly >= date && i.TimeOnly > time));
        }
    }


    private List<DateAndTimeOnly> CreateDatabaseData(IDocumentStore store)
    {
        TimeOnly timeOnly = new TimeOnly(0, 0, 0, 234);
        DateOnly dateOnly;
        DateTime dt;
        var database = Enumerable.Range(0, 1000).Select(
            i => new DateAndTimeOnly() { TimeOnly = timeOnly.AddMinutes(i), DateOnly = dateOnly.AddDays(i), DateTime = DateTime.Now }).ToList();
        using var bulkInsert = store.BulkInsert();
        database.ForEach(i => bulkInsert.Store(i));
        return database;
    }

    private class DateAndTimeOnly
    {
        public DateOnly DateOnly { get; set; }
        public TimeOnly TimeOnly { get; set; }
        public DateTime DateTime { get; set; }
    }

    private class DateAndTimeOnlyIndex : AbstractIndexCreationTask<DateAndTimeOnly, DateAndTimeOnlyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public DateOnly DateOnly { get; set; }
            
            public int Year { get; set; }

            public DateOnly DateOnlyString { get; set; }

            public TimeOnly TimeOnlyString { get; set; }
            public TimeOnly TimeOnly { get; set; }
        }

        public DateAndTimeOnlyIndex()
        {
            Map = dates => from date in dates

                //let x = date.DateTime
                select new IndexEntry()
                {
                    // year =  AsDateOnly(x).Year,

                    DateOnly = date.DateOnly//, TimeOnly = AsTimeOnly(date.TimeOnly), DateOnlyString = date.DateOnly, TimeOnlyString = date.TimeOnly
                };
        }
    }

    private class DateAndTimeOnlyIndexWithDateTime : AbstractIndexCreationTask<DateAndTimeOnly, DateAndTimeOnlyIndex.IndexEntry>
    {
        public class IndexEntry
        {
            public DateOnly DateOnly { get; set; }
            public int Year { get; set; }

            public DateOnly DateOnlyString { get; set; }

            public TimeOnly TimeOnlyString { get; set; }
            public TimeOnly TimeOnly { get; set; }
        }

        public DateAndTimeOnlyIndexWithDateTime()
        {
            Map = dates => from date in dates
                let x = date.DateTime
                select new IndexEntry()
                {
                    Year = AsDateOnly(x).Year,
                    DateOnly = AsDateOnly(new DateOnly(2020,12,24)),
                    TimeOnly = AsTimeOnly(date.TimeOnly),
                    DateOnlyString = date.DateOnly,
                    TimeOnlyString = date.TimeOnly
                };
        }
    }
}
