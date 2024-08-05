using System;
using System.Globalization;
using System.Text;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Utils
{
    public unsafe class TimeParsing : NoDisposalNeeded
    {
        public TimeParsing(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("2016-10-05T21:07:32.2082285Z")]
        [InlineData("2016-10-05T21:07:32.2082285")]
        [InlineData("2016-10-05T21:07:32")]
        public void CanParseValidDates(string dt)
        {
            var expected = DateTime.ParseExact(dt, DefaultFormat.OnlyDateTimeFormat, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            {
                DateTime time;
                DateTimeOffset dto;
                Assert.Equal(LazyStringParser.Result.DateTime,
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out time, out dto, properlyParseThreeDigitsMilliseconds: true));
                Assert.Equal(expected, time);
            }
        }

        [Theory]
        [InlineData("21:07:32.2082285")]
        [InlineData("21:07:32")]
        [InlineData("2.21:07:32")]
        [InlineData("-2.21:07:32")]
        [InlineData("2.21:07:32.232")]
        [InlineData("333.21:07:32.232")]
        [InlineData("12:11:02.")]
        public void CanParseValidTimeSpans(string dt)
        {
            var expected = TimeSpan.ParseExact(dt,"c", CultureInfo.InvariantCulture);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            fixed (char* str = dt)
            {
                TimeSpan ts;
                Assert.True(LazyStringParser.TryParseTimeSpan(buffer, bytes.Length, out ts));
                Assert.Equal(expected, ts);

                Assert.True(LazyStringParser.TryParseTimeSpan(str, dt.Length, out ts));
                Assert.Equal(expected, ts);
            }
        }

        [Theory]
        [InlineData("21:07:32 some text")]
        [InlineData("2.21:07:32 some text")]
        [InlineData("333.21:07:32.232 some text")]
        [InlineData("00:00:00 some text.")]
        [InlineData("00:00:00. some text")]
        public void WillNotParseAsTimeSpan(string dt)
        {
            TimeSpan expected;
            var result = TimeSpan.TryParseExact(dt, "c", CultureInfo.InvariantCulture, out expected);
            Assert.False(result);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            fixed (char* str = dt)
            {
                TimeSpan ts;
                Assert.False(LazyStringParser.TryParseTimeSpan(buffer, bytes.Length, out ts));
                Assert.Equal(expected, ts);

                Assert.False(LazyStringParser.TryParseTimeSpan(str, dt.Length, out ts));
                Assert.Equal(expected, ts);
            }
        }

        [Theory]
        [InlineData("2016-10-05T21:07:32.2082285+03:00")]
        [InlineData("2016-10-05T21:17:32.2082285+01:00")]
        public void CanParseValidDatesTimeOffset(string dt)
        {
            var expected = DateTimeOffset.ParseExact(dt, DefaultFormat.DateTimeOffsetFormatsToWrite, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            {
                DateTime time;
                DateTimeOffset dto;
                Assert.Equal(LazyStringParser.Result.DateTimeOffset,
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out time, out dto, properlyParseThreeDigitsMilliseconds: true));
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
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out time, out dto, properlyParseThreeDigitsMilliseconds: true));
            }
        }
        
        [Theory]
        [InlineData("1998-02-09", 1998, 2, 9)]
        [InlineData("0001-12-10", 1, 12, 10)]
        [InlineData("2022-02-14", 2022, 2, 14)]
        [InlineData("5999-01-01", 5999, 1, 1)]

        public void DateOnly(string date, int yyyy, int mm, int dd)
        {
            var bytes = date.AsSpan();
            fixed (char* buffer = bytes)
            {
                Assert.True(LazyStringParser.TryParseDateOnly(buffer, bytes.Length, out var result));
                Assert.True(result.Equals(new DateOnly(yyyy,mm,dd)));
            }
        }

        [Theory]
        [InlineData("20:59:12.9990000", 20, 59, 12, 999)]
        [InlineData("21:38:32.9120000", 21, 38, 32, 912)]
        [InlineData("23:59:00", 23, 59,0,0)]
        [InlineData("23:01:09", 23, 1,9,0)]
        public void TimeOnly(string date, int hh, int mm, int ss, int ms)
        {
            var bytes = date.AsSpan();
            fixed (char* buffer = bytes)
            {
                Assert.True(LazyStringParser.TryParseTimeOnly(buffer, bytes.Length, out var result));
                Assert.True(result.Equals(new TimeOnly(hh, mm, ss, ms)));
            }
        }



    }
}
