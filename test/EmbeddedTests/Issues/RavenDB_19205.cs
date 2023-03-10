using System;
using System.Linq;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests.Issues;

public class RavenDB_19205 : EmbeddedTestBase
{
    //https://github.com/dotnet/runtime/pull/67666
    [Fact]
    public void DatePrecisionTest()
    {
        var options = CopyServerAndCreateOptions();
        using var embedded = new EmbeddedServer();

        embedded.StartServer(options);

        using var store = embedded.GetDocumentStore("RavenDB_19205");
        var mq = new DateContainer()
        {
            DateTime = new DateTime(1, 1, 1, 1, 1, 1, millisecond: 555
#if NET7_0_OR_GREATER
                , microsecond: 321
#endif
            ),
            TimeSpan = new TimeSpan(1, 1, 1, 1, milliseconds: 555
#if NET7_0_OR_GREATER
                , microseconds: 321
#endif

            ),
            DateTimeOffset = new DateTimeOffset(1, 1, 1, 1, 1, 1, millisecond: 521,
#if NET7_0_OR_GREATER
                microsecond: 321,
#endif

                new TimeSpan(1, 1, 0)),
#if NET6_0_OR_GREATER
            TimeOnly = new TimeOnly(1, 1, 1, millisecond: 555
#if NET7_0_OR_GREATER
                , microsecond: 321
#endif
            )
#endif
        };


        using (var session = store.OpenSession())
        {
            session.Store(mq);
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var date = session.Query<DateContainer>().Single();
            Assert.True((bool)date.DateTime.Equals(mq.DateTime), $"{nameof(date.DateTime)}");
            Assert.True((bool)date.TimeSpan.Equals(mq.TimeSpan), $"{nameof(date.TimeSpan)}");
            Assert.True((bool)date.DateTimeOffset.Equals(mq.DateTimeOffset), $"{nameof(date.DateTimeOffset)}");
#if NET6_0_OR_GREATER
            Assert.True((bool)date.TimeOnly.Equals(mq.TimeOnly), $"{nameof(date.TimeOnly)}");
#endif
        }
    }

    private class DateContainer
    {
        public DateTime DateTime { get; set; }
        public TimeSpan TimeSpan { get; set; }
        public DateTimeOffset DateTimeOffset { get; set; }
#if NET6_0_OR_GREATER
        public TimeOnly TimeOnly { get; set; }
#endif
    }
}
