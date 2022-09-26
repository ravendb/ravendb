using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Microsoft.Azure.Documents.SystemFunctions;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;
using TimeOnly = System.TimeOnly;

namespace SlowTests.Issues;

public class RDBC_631 : RavenTestBase
{
    public RDBC_631(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanProjectOnlyDateOnly()
    {
        var datesOnly = new List<DateOnly>();
        using var store = GetDocumentStore();
        {
            using var bulkInsert = store.BulkInsert();
            foreach (var i in Enumerable.Range(0, 100))
            {
                var date = new DateOnly(2022, (i / 28) + 1, (i % 28) + 1);
                bulkInsert.Store(new Mock<DateOnly>() {Date = date});
                datesOnly.Add(date);
            }
        }
        new DateOnlyIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        {
            using var session = store.OpenSession();
            var datesFromRaven = session.Query<Mock<DateOnly>, DateOnlyIndex>().Select(i => i.Date).ToList();
            datesOnly.Sort();
            datesFromRaven.Sort();
            Assert.True(datesFromRaven.SequenceEqual(datesOnly));
        }
    }
    
    [Fact]
    public void CanProjectOnlyTimeOnly()
    {
        var datesOnly = new List<TimeOnly>();
        using var store = GetDocumentStore();
        {
            using var bulkInsert = store.BulkInsert();
            foreach (var i in Enumerable.Range(0, 100))
            {
                var date = new TimeOnly(i);
                bulkInsert.Store(new Mock<TimeOnly>() {Date = date});
                datesOnly.Add(date);
            }
        }
        new TimeOnlyIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        {
            using var session = store.OpenSession();
            var datesFromRaven = session.Query<Mock<TimeOnly>, TimeOnlyIndex>().Select(i => i.Date).ToList();
            datesOnly.Sort();
            datesFromRaven.Sort();
            Assert.True(datesFromRaven.SequenceEqual(datesOnly));
        }
    }
    
    private class Mock<T>
    {
        public string Id { get; set; }
        public T Date { get; set; }
    }

    private class TimeOnlyIndex : AbstractIndexCreationTask<Mock<TimeOnly>>
    {
        public TimeOnlyIndex()
        {
            Map = mocks => mocks.Select(i => new Mock<TimeOnly>() {Id = i.Id, Date = i.Date});
        }
    }
    
    private class DateOnlyIndex : AbstractIndexCreationTask<Mock<DateOnly>>
    {
        public DateOnlyIndex()
        {
            Map = mocks => mocks.Select(i => new Mock<DateOnly>() {Id = i.Id, Date = i.Date});
        }
    }


}
