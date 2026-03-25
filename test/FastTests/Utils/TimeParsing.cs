using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using FastTests.Voron.FixedSize;
using Sparrow;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Utils
{
    public unsafe class TimeParsing(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Core)]
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

        [RavenTheory(RavenTestCategory.Core)]
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

        [RavenTheory(RavenTestCategory.Core)]
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

        [RavenTheory(RavenTestCategory.Core)]
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
        
        [RavenTheory(RavenTestCategory.Core)]
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
        
        [RavenTheory(RavenTestCategory.Core)]
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

        [RavenTheory(RavenTestCategory.Core)]
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

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("2024-12-13T02:38:42.786481Z")] // 27
        [InlineData("2024-12-13T02:38:42.7864811")] // 27
        [InlineData("2024-12-13T02:38:42.78648Z")] //26
        [InlineData("2024-12-13T02:38:42.786488")] //26
        [InlineData("2024-12-13T02:38:42.7864Z")] //25
        [InlineData("2024-12-13T02:38:42.78644")] //25
        [InlineData("2024-12-13T02:38:42.786Z")] // 24
        [InlineData("2024-12-13T02:38:42.7868")] // 24
        [InlineData("2024-12-13T02:38:42.78Z")] //23
        [InlineData("2024-12-13T02:38:42.788")] //23
        [InlineData("2024-12-13T02:38:42.7Z")] // 22
        [InlineData("2024-12-13T02:38:42.77")] // 22
        [InlineData("2024-12-13T02:38:42.7")] // 21
        [InlineData("2024-12-13T02:38:42Z")]
        public void CanParseValidDatesWithTrailingZerosInMillisecondsPart(string dt)
        {
            var expected = DateTime.ParseExact(dt, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);

            var bytes = Encoding.UTF8.GetBytes(dt);
            fixed (byte* buffer = bytes)
            {
                Assert.Equal(LazyStringParser.Result.DateTime,
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out DateTime time, out _, properlyParseThreeDigitsMilliseconds: true));

                Assert.Equal(expected.Kind, time.Kind);
                Assert.Equal(expected, time);
            }

            fixed (char* buffer = dt)
            {
                Assert.Equal(LazyStringParser.Result.DateTime,
                    LazyStringParser.TryParseDateTime(buffer, bytes.Length, out DateTime time, out _, properlyParseThreeDigitsMilliseconds: true));

                Assert.Equal(expected.Kind, time.Kind);
                Assert.Equal(expected, time);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("2024-12-13T02:38:42.7864810Z")]
        [InlineData("2024-12-13T02:38:42.7864800Z")]
        [InlineData("2024-12-13T02:38:42.7864000Z")]
        [InlineData("2024-12-13T02:38:42.7860000Z")]
        [InlineData("2024-12-13T02:38:42.7800000Z")]
        [InlineData("2024-12-13T02:38:42.7000000Z")]
        [InlineData("2024-12-13T02:38:42.0000000Z")]
        public void CanParseValidUtcDatesWithTrailingZerosInMillisecondsPart_DifferentFormats(string dt)
        {
            var expected = DateTime.ParseExact(dt, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind);

            var formatsToRead = new Dictionary<string, DateTimeKind>
            {
                {DefaultFormat.DateTimeFormatsToRead[0], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[3], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[5], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[6], DateTimeKind.Utc},
            };

            foreach (var dateTimeFormat in formatsToRead)
            {
                string tested = expected.ToString(dateTimeFormat.Key);

                var expectedAfterFormatting = DateTime.ParseExact(tested, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                    DateTimeStyles.RoundtripKind);

                var bytes = Encoding.UTF8.GetBytes(tested);
                fixed (byte* buffer = bytes)
                {
                    Assert.Equal(LazyStringParser.Result.DateTime,
                        LazyStringParser.TryParseDateTime(buffer, bytes.Length, out DateTime time, out _, properlyParseThreeDigitsMilliseconds: true));
                    Assert.Equal(expectedAfterFormatting, time);
                }

                fixed (char* buffer = tested)
                {
                    Assert.Equal(LazyStringParser.Result.DateTime,
                        LazyStringParser.TryParseDateTime(buffer, bytes.Length, out DateTime time, out _, properlyParseThreeDigitsMilliseconds: true));
                    Assert.Equal(expectedAfterFormatting, time);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineDataWithRandomSeed]
        public void CanParseValidRandomDate(int seed)
        {
            var r = new Random(seed);

            var dt = GetRandomDate(r);

            var formatsToRead = new Dictionary<string, DateTimeKind>
            {
                {DefaultFormat.DateTimeFormatsToRead[0], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[1], DateTimeKind.Unspecified},
                {DefaultFormat.DateTimeFormatsToRead[2], DateTimeKind.Local},
                {DefaultFormat.DateTimeFormatsToRead[3], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[4], DateTimeKind.Unspecified},
                {DefaultFormat.DateTimeFormatsToRead[5], DateTimeKind.Utc},
                {DefaultFormat.DateTimeFormatsToRead[6], DateTimeKind.Utc},
            };
            
            Assert.Equal(formatsToRead.Count, DefaultFormat.DateTimeFormatsToRead.Length);

            foreach (var dateTimeFormat in formatsToRead)
            {
                string tested = dt.ToString(dateTimeFormat.Key);

                var bytes = Encoding.UTF8.GetBytes(tested);
                fixed (byte* buffer = bytes)
                {
                    var parseResult = LazyStringParser.TryParseDateTime(buffer, bytes.Length, out var dateTime, out var dateTimeOffset, properlyParseThreeDigitsMilliseconds: true);
                    
                    Assert.True(parseResult != LazyStringParser.Result.Failed, $"parseResult: {parseResult}, tested value: {tested}");

                    switch (parseResult)
                    {
                        case LazyStringParser.Result.DateTime:

                            Assert.Equal(tested, dateTime.ToString(dateTimeFormat.Key));
                            break;
                        case LazyStringParser.Result.DateTimeOffset:

                            Assert.Equal(tested, dateTimeOffset.ToString(dateTimeFormat.Key));
                            break;
                    }
                }

                fixed (char* buffer = tested)
                {
                    var parseResult = LazyStringParser.TryParseDateTime(buffer, bytes.Length, out var dateTime, out var dateTimeOffset, properlyParseThreeDigitsMilliseconds: true);

                    Assert.True(parseResult != LazyStringParser.Result.Failed, $"parseResult: {parseResult}, tested value: {tested}");

                    switch (parseResult)
                    {
                        case LazyStringParser.Result.DateTime:

                            Assert.Equal(tested, dateTime.ToString(dateTimeFormat.Key));
                            break;
                        case LazyStringParser.Result.DateTimeOffset:

                            Assert.Equal(tested, dateTimeOffset.ToString(dateTimeFormat.Key));
                            break;
                    }
                }
            }
        }

        private static DateTime GetRandomDate(Random random, int minYear = 1900, int maxYear = 2099)
        {
            var year = random.Next(minYear, maxYear);
            var month = random.Next(1, 12);
            var noOfDaysInMonth = DateTime.DaysInMonth(year, month);
            var day = random.Next(1, noOfDaysInMonth);

            DateTime randomDate = new DateTime(year, month, day);

            randomDate = randomDate.AddMilliseconds(random.Next(0, 9999999));
            return randomDate;
        }
    }
}
