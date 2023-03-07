using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19205 : RavenTestBase
{
    public RavenDB_19205(ITestOutputHelper output) : base(output)
    {
    }

    //https://github.com/dotnet/runtime/pull/67666
    [Fact]
    public void DatePrecisionTest()
    {
        using var store = GetDocumentStore();
        var mq = new DateContainer(
            DateTime: new DateTime(1, 1, 1, 1, 1, 1, millisecond: 555, microsecond: 321),
            TimeSpan: new TimeSpan(1, 1, 1, 1, milliseconds: 555, microseconds: 321),
            DateTimeOffset: new DateTimeOffset(1, 1, 1, 1, 1, 1, millisecond: 521, microsecond: 321, new TimeSpan(1, 1, 0)),
            TimeOnly: new TimeOnly(1, 1, 1, millisecond: 555, microsecond: 321)
        );


        using (var session = store.OpenSession())
        {
            session.Store(mq);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var date = session.Query<DateContainer>().Single();
            Assert.True(date.DateTime.Equals(mq.DateTime), $"{nameof(date.DateTime)}");
            Assert.True(date.TimeSpan.Equals(mq.TimeSpan), $"{nameof(date.TimeSpan)}");
            Assert.True(date.DateTimeOffset.Equals(mq.DateTimeOffset), $"{nameof(date.DateTimeOffset)}");
            Assert.True(date.TimeOnly.Equals(mq.TimeOnly), $"{nameof(date.TimeOnly)}");
        }
    }

    private record DateContainer(DateTime DateTime, TimeSpan TimeSpan, DateTimeOffset DateTimeOffset, TimeOnly TimeOnly);
}
