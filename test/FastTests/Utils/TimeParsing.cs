using System;
using System.Globalization;
using System.Text;
using Raven.Abstractions;
using Raven.Server.Utils;
using Xunit;

namespace FastTests.Utils
{
    public unsafe class TimeParsing : NoDisposalNeeded
    {
        [Theory]
        [InlineData("2016-10-05T21:07:32.2082285Z")]
        [InlineData("2016-10-05T21:07:32.2082285")]
        [InlineData("2016-10-05T21:07:32")]
        public void CanParseValidDates(string dt)
        {
            var expected = DateTime.ParseExact(dt, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            {
                DateTime time;
                DateTimeOffset dto;
                Assert.Equal(LazyStringParser.Result.DateTime,
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out time, out dto));
                Assert.Equal(expected, time);
            }
        }

        [Theory]
        [InlineData("21:07:32.2082285")]
        [InlineData("21:07:32")]
        [InlineData("2.21:07:32")]
        [InlineData("-2.21:07:32")]
        [InlineData("2.21:07:32.232")]
        public void CanParseValidTimeSpans(string dt)
        {
            var expected = TimeSpan.ParseExact(dt,"c", CultureInfo.InvariantCulture);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            {
                TimeSpan ts;
                Assert.True(LazyStringParser.TryParseTimeSpan(buffer, bytes.Length, out ts));
                Assert.Equal(expected, ts);
            }
        }

        [Theory]
        [InlineData("2016-10-05T21:07:32.2082285+03:00")]
        [InlineData("2016-10-05T21:17:32.2082285+01:00")]
        public void CanParseValidDatesTimeOffset(string dt)
        {
            var expected = DateTimeOffset.ParseExact(dt, "o", CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            {
                DateTime time;
                DateTimeOffset dto;
                Assert.Equal(LazyStringParser.Result.DateTimeOffset,
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out time, out dto));
                Assert.Equal(expected, dto);
            }
        }



        [Theory]
        [InlineData("2016-10-05T")]
        [InlineData("2016-10-05T21:17:32.2082285+01:00,ad")]
        [InlineData("2016-10-05T21:17:3")]
        [InlineData("2016-10-05T21:17:32.2082285+01:00:00")]
        public void InvalidData(string dt)
        {
            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            {
                DateTime time;
                DateTimeOffset dto;
                Assert.Equal(LazyStringParser.Result.Failed,
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out time, out dto));
            }
        }
    }
}