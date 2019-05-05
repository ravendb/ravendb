using System;
using System.Globalization;
using System.Linq;
using Raven.Server.Documents.Queries.AST;
using Xunit;

namespace FastTests.Server.Documents
{
    public class TimeSeriesRangeTests : NoDisposalNeeded
    {
        [Theory]
        [InlineData("1s", "2019-05-05T06:03:51.1077101Z", "2019-05-05T06:03:51.0000000Z", "2019-05-05T06:03:52.0000000Z")]
        [InlineData("1m", "2019-05-05T06:03:51.1077101Z", "2019-05-05T06:03:00.0000000Z", "2019-05-05T06:04:00.0000000Z")]
        [InlineData("1h", "2019-05-05T06:03:51.1077101Z", "2019-05-05T06:00:00.0000000Z", "2019-05-05T07:00:00.0000000Z")]
        [InlineData("1d", "2019-05-05T06:03:51.1077101Z", "2019-05-05T00:00:00.0000000Z", "2019-05-06T00:00:00.0000000Z")]
        [InlineData("1 month", "2019-05-05T06:03:51.1077101Z", "2019-05-01T00:00:00.0000000Z", "2019-06-01T00:00:00.0000000Z")]
        public void CanGetRangeStartAndNext(string rangeStr, string dateStr, string startStr, string nextStr)
        {
            var rangeSpec = TimeSeriesFunction.ParseRangeFromString(rangeStr);
            var date = DateTime.ParseExact(dateStr, "o", CultureInfo.InvariantCulture).ToUniversalTime();

            var start = rangeSpec.GetRangeStart(date);

            Assert.Equal(
                DateTime.ParseExact(startStr, "o", CultureInfo.InvariantCulture).ToUniversalTime(),
                start);

            var next = rangeSpec.GetNextRangeStart(start);

            Assert.Equal(
                DateTime.ParseExact(nextStr, "o", CultureInfo.InvariantCulture).ToUniversalTime(),
                next);

        }
    }
}
