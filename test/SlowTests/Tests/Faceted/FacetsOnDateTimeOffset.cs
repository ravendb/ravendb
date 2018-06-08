using System;
using FastTests;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Tests.Faceted
{
    public class FacetsOnDateTimeOffset : NoDisposalNeeded
    {
        private class ClassWithDateTimeOffset
        {
            public DateTime Date { get; set; }
            public DateTimeOffset DateTimeOffset { get; set; }
            public DateTimeOffset? NullableDateTimeOffset { get; set; }
        }

        [Fact]
        public void FacetShouldWorkWithDateOffset()
        {
            var now = new DateTime(2017, 1, 2);
            var min = DateTime.MinValue;
            var actual = RangeFacet<ClassWithDateTimeOffset>.Parse(c => c.Date > min && c.Date < now);

            Assert.Equal("Date > '0001-01-01T00:00:00.0000000' and Date < '2017-01-02T00:00:00.0000000'", actual);

        }

        [Fact]
        public void FacetShouldWorkWithDateTimeOffset()
        {
            var now = new DateTimeOffset(2017, 1, 2, 0, 0, 0, TimeSpan.Zero);
            var min = DateTimeOffset.MinValue;
            var actual = RangeFacet<ClassWithDateTimeOffset>.Parse(c => c.DateTimeOffset > min && c.DateTimeOffset < now);

            Assert.Equal("DateTimeOffset > '0001-01-01T00:00:00.0000000Z' and DateTimeOffset < '2017-01-02T00:00:00.0000000Z'", actual);
        }

        [Fact]
        public void FacetShouldWorkWithNullableDateTimeOffset()
        {
            DateTimeOffset? now = new DateTimeOffset(2017, 1, 2, 0, 0, 0, TimeSpan.Zero);
            DateTimeOffset? min = DateTimeOffset.MinValue;
            var actual = RangeFacet<ClassWithDateTimeOffset>.Parse(c => c.NullableDateTimeOffset > min && c.NullableDateTimeOffset < now);

            Assert.Equal("NullableDateTimeOffset > '0001-01-01T00:00:00.0000000Z' and NullableDateTimeOffset < '2017-01-02T00:00:00.0000000Z'", actual);
        }

        [Fact]
        public void IdeallyTheVariableWouldNotNeedToBeANullable()
        {
            DateTimeOffset now = new DateTimeOffset(2017, 1, 2, 0, 0, 0, TimeSpan.Zero);
            DateTimeOffset min = DateTimeOffset.MinValue;
            var actual = RangeFacet<ClassWithDateTimeOffset>.Parse(c => c.NullableDateTimeOffset > min && c.NullableDateTimeOffset < now);

            Assert.Equal("NullableDateTimeOffset > '0001-01-01T00:00:00.0000000Z' and NullableDateTimeOffset < '2017-01-02T00:00:00.0000000Z'", actual);
        }

        [Fact]
        public void IdeallyIWouldNotNeedAVariable()
        {
            var actual = RangeFacet<ClassWithDateTimeOffset>.Parse(c => c.DateTimeOffset > DateTimeOffset.MinValue && c.DateTimeOffset < new DateTimeOffset(2017, 1, 2, 0, 0, 0, TimeSpan.Zero));

            Assert.Equal("DateTimeOffset > '0001-01-01T00:00:00.0000000Z' and DateTimeOffset < '2017-01-02T00:00:00.0000000Z'", actual);
        }
    }
}
