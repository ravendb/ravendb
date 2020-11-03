// -----------------------------------------------------------------------
//  <copyright file="RavenDateTimeExtensions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Sparrow.Extensions
{
    public static class RavenDateTimeExtensions
    {
        private const long InitialJavaScriptDateTicks = 621355968000000000;

        // Number of 100ns ticks per time unit
        private const long TicksPerMillisecond = 10000;

        private const long TicksPerSecond = TicksPerMillisecond * 1000;
        private const long TicksPerMinute = TicksPerSecond * 60;
        private const long TicksPerHour = TicksPerMinute * 60;
        private const long TicksPerDay = TicksPerHour * 24;

        // Number of milliseconds per time unit
        private const int MillisPerSecond = 1000;

        private const int MillisPerMinute = MillisPerSecond * 60;
        private const int MillisPerHour = MillisPerMinute * 60;
        private const int MillisPerDay = MillisPerHour * 24;

        // Number of days in a non-leap year
        private const int DaysPerYear = 365;

        // Number of days in 4 years
        private const int DaysPer4Years = DaysPerYear * 4 + 1;       // 1461

        // Number of days in 100 years
        private const int DaysPer100Years = DaysPer4Years * 25 - 1;  // 36524

        // Number of days in 400 years
        private const int DaysPer400Years = DaysPer100Years * 4 + 1; // 146097

        // Number of days from 1/1/0001 to 12/31/1600
        private const int DaysTo1601 = DaysPer400Years * 4;          // 584388

        // Number of days from 1/1/0001 to 12/30/1899
        private const int DaysTo1899 = DaysPer400Years * 4 + DaysPer100Years * 3 - 367;

        // Number of days from 1/1/0001 to 12/31/9999
        private const int DaysTo10000 = DaysPer400Years * 25 - 366;  // 3652059

        internal const long MinTicks = 0;
        internal const long MaxTicks = DaysTo10000 * TicksPerDay - 1;

        private static readonly int[] DaysToMonth365 = {
            0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};

        private static readonly int[] DaysToMonth366 = {
            0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};

        private static readonly char[][] _fourDigits = CreateFourDigitsCache();
        private static readonly byte[][] _fourDigitsByte = CreateFourDigitsByteCache();

        private static char[][] CreateFourDigitsCache()
        {
            var c = new char[10000][];
            for (int i = 0; i < 10000; i++)
            {
                c[i] = new[]
                {
                    (char) (i/1000 + '0'),
                    (char) ((i%1000)/100 + '0'),
                    (char) ((i%100)/10 + '0'),
                    (char) (i%10 + '0')
                };
            }
            return c;
        }

        private static byte[][] CreateFourDigitsByteCache()
        {
            var c = new byte[10000][];
            for (int i = 0; i < 10000; i++)
            {
                c[i] = new[]
                {
                    (byte) (i/1000 + '0'),
                    (byte) ((i%1000)/100 + '0'),
                    (byte) ((i%100)/10 + '0'),
                    (byte) (i%10 + '0')
                };
            }
            return c;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe void ProcessDefaultRavenFormat(long ticks, char* chars)
        {
            // n = number of days since 1/1/0001
            int n = (int)(ticks / TicksPerDay);
            // y400 = number of whole 400-year periods since 1/1/0001
            int y400 = n / DaysPer400Years;
            // n = day number within 400-year period
            n -= y400 * DaysPer400Years;
            // y100 = number of whole 100-year periods within 400-year period
            int y100 = n / DaysPer100Years;
            // Last 100-year period has an extra day, so decrement result if 4
            if (y100 == 4)
                y100 = 3;
            // n = day number within 100-year period
            n -= y100 * DaysPer100Years;
            // y4 = number of whole 4-year periods within 100-year period
            int y4 = n / DaysPer4Years;
            // n = day number within 4-year period
            n -= y4 * DaysPer4Years;
            // y1 = number of whole years within 4-year period
            int y1 = n / DaysPerYear;
            // Last year has an extra day, so decrement result if 4
            if (y1 == 4)
                y1 = 3;
            // If year was requested, compute and return it
            var year = y400 * 400 + y100 * 100 + y4 * 4 + y1 + 1;

            // n = day number within year
            n -= y1 * DaysPerYear;
            // Leap year calculation looks different from IsLeapYear since y1, y4,
            // and y100 are relative to year 1, not year 0
            bool leapYear = y1 == 3 && (y4 != 24 || y100 == 3);
            int[] days = leapYear ? DaysToMonth366 : DaysToMonth365;
            // All months have less than 32 days, so n >> 5 is a good conservative
            // estimate for the month
            int month = n >> 5 + 1;
            // m = 1-based month number
            while (n >= days[month])
                month++;
            // If month was requested, return it

            // Return 1-based day-of-month
            var day = n - days[month - 1] + 1;

            var v = _fourDigits[year];
            chars[0] = v[0];
            chars[1] = v[1];
            chars[2] = v[2];
            chars[3] = v[3];
            chars[4] = '-';
            v = _fourDigits[month];
            chars[5] = v[2];
            chars[5 + 1] = v[3];
            chars[7] = '-';
            v = _fourDigits[day];
            chars[8] = v[2];
            chars[8 + 1] = v[3];
            chars[10] = 'T';
            v = _fourDigits[(ticks / TicksPerHour) % 24];
            chars[11] = v[2];
            chars[11 + 1] = v[3];
            chars[13] = ':';
            v = _fourDigits[(ticks / TicksPerMinute) % 60];
            chars[14] = v[2];
            chars[14 + 1] = v[3];
            chars[16] = ':';
            v = _fourDigits[(ticks / TicksPerSecond) % 60];
            chars[17] = v[2];
            chars[17 + 1] = v[3];
            chars[19] = '.';

            long fraction = (ticks % 10000000);
            v = _fourDigits[fraction / 10000];
            chars[20] = v[1];
            chars[21] = v[2];
            chars[22] = v[3];

            fraction = fraction % 10000;

            v = _fourDigits[fraction];
            chars[23] = v[0];
            chars[24] = v[1];
            chars[25] = v[2];
            chars[26] = v[3];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void ProcessDefaultRavenFormat(long ticks, byte* chars)
        {
            // n = number of days since 1/1/0001
            int n = (int)(ticks / TicksPerDay);
            // y400 = number of whole 400-year periods since 1/1/0001
            int y400 = n / DaysPer400Years;
            // n = day number within 400-year period
            n -= y400 * DaysPer400Years;
            // y100 = number of whole 100-year periods within 400-year period
            int y100 = n / DaysPer100Years;
            // Last 100-year period has an extra day, so decrement result if 4
            if (y100 == 4)
                y100 = 3;
            // n = day number within 100-year period
            n -= y100 * DaysPer100Years;
            // y4 = number of whole 4-year periods within 100-year period
            int y4 = n / DaysPer4Years;
            // n = day number within 4-year period
            n -= y4 * DaysPer4Years;
            // y1 = number of whole years within 4-year period
            int y1 = n / DaysPerYear;
            // Last year has an extra day, so decrement result if 4
            if (y1 == 4)
                y1 = 3;
            // If year was requested, compute and return it
            var year = y400 * 400 + y100 * 100 + y4 * 4 + y1 + 1;

            // n = day number within year
            n -= y1 * DaysPerYear;
            // Leap year calculation looks different from IsLeapYear since y1, y4,
            // and y100 are relative to year 1, not year 0
            bool leapYear = y1 == 3 && (y4 != 24 || y100 == 3);
            int[] days = leapYear ? DaysToMonth366 : DaysToMonth365;
            // All months have less than 32 days, so n >> 5 is a good conservative
            // estimate for the month
            int month = n >> 5 + 1;
            // m = 1-based month number
            while (n >= days[month])
                month++;
            // If month was requested, return it

            // Return 1-based day-of-month
            var day = n - days[month - 1] + 1;

            var v = _fourDigitsByte[year];
            chars[0] = v[0];
            chars[1] = v[1];
            chars[2] = v[2];
            chars[3] = v[3];
            chars[4] = (byte)'-';
            v = _fourDigitsByte[month];
            chars[5] = v[2];
            chars[5 + 1] = v[3];
            chars[7] = (byte)'-';
            v = _fourDigitsByte[day];
            chars[8] = v[2];
            chars[8 + 1] = v[3];
            chars[10] = (byte)'T';
            v = _fourDigitsByte[(ticks / TicksPerHour) % 24];
            chars[11] = v[2];
            chars[11 + 1] = v[3];
            chars[13] = (byte)':';
            v = _fourDigitsByte[(ticks / TicksPerMinute) % 60];
            chars[14] = v[2];
            chars[14 + 1] = v[3];
            chars[16] = (byte)':';
            v = _fourDigitsByte[(ticks / TicksPerSecond) % 60];
            chars[17] = v[2];
            chars[17 + 1] = v[3];
            chars[19] = (byte)'.';

            long fraction = (ticks % 10000000);
            v = _fourDigitsByte[fraction / 10000];
            chars[20] = v[1];
            chars[21] = v[2];
            chars[22] = v[3];

            fraction = fraction % 10000;

            v = _fourDigitsByte[fraction];
            chars[23] = v[0];
            chars[24] = v[1];
            chars[25] = v[2];
            chars[26] = v[3];
        }

        /// <summary>
        /// This function Processes the to string format of the form "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff" for date times in
        /// invariant culture scenarios. This implementation takes 20% of the time of a regular .ToString(format) call
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="isUtc"></param>
        /// <returns></returns>
        public static unsafe string GetDefaultRavenFormat(this DateTime dt, bool isUtc)
        {
            ValidateDate(dt, isUtc);

            string result = new string('Z', 27 + (isUtc ? 1 : 0));

            var ticks = dt.Ticks;

            fixed (char* chars = result)
            {
                ProcessDefaultRavenFormat(ticks, chars);
            }

            return result;
        }

        public static string GetDefaultRavenFormat(this DateTime dt)
        {
            return GetDefaultRavenFormat(dt, dt.Kind == DateTimeKind.Utc);
        }

        /// <summary>
        /// This function Processes the to string format of the form "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff" for date times in
        /// invariant culture scenarios. This implementation takes 20% of the time of a regular .ToString(format) call
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="isUtc"></param>
        /// <returns></returns>
        public static unsafe int GetDefaultRavenFormat(this DateTime dt, JsonOperationContext context, out AllocatedMemoryData memory, bool isUtc = false)
        {
            ValidateDate(dt, isUtc);

            int size = 27 + (isUtc ? 1 : 0);
            var ticks = dt.Ticks;

            memory = context.GetMemory(size);

            var ptr = memory.Address;
            ProcessDefaultRavenFormat(ticks, ptr);

            if (isUtc)
                ptr[size - 1] = (byte)'Z';

            return size;
        }

        /// <summary>
        /// This function Processes the to string format of the form "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff" for date times in
        /// invariant culture scenarios. This implementation takes 20% of the time of a regular .ToString(format) call
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="isUtc"></param>
        /// <returns></returns>
        public static unsafe int GetDefaultRavenFormat(this DateTime dt, byte* ptr, int ptrSize, bool isUtc = false)
        {
            ValidateDate(dt, isUtc);

            int size = 27 + (isUtc ? 1 : 0);
            if (ptrSize < size)
                goto Error;

            var ticks = dt.Ticks;

            ProcessDefaultRavenFormat(ticks, ptr);

            if (isUtc)
                ptr[size - 1] = (byte)'Z';

            return size;

        Error:
            return ThrowMemoryIsNotBigEnough();
        }

        [Conditional("DEBUG")]
        internal static void ValidateDate(DateTime dt, bool isUtc)
        {
            if (dt.Kind == DateTimeKind.Utc && isUtc == false)
                throw new InvalidOperationException("Date is in UTC, but will not be formatted into UTC");

            if (dt.Kind == DateTimeKind.Local && isUtc)
                throw new InvalidOperationException("Date is not in UTC, but will be formatted into UTC");
        }

        private static int ThrowMemoryIsNotBigEnough()
        {
            throw new ArgumentException($"The memory passed to {nameof(GetDefaultRavenFormat)} is not big enough, we require at least 28 bytes to operate. This exception should never ever happen.");
        }

        public static DateTime ParseDateMicrosoft(string text)
        {
            var value = text.Substring(6, text.Length - 8);

            var index = value.IndexOf('+', 1);

            if (index == -1)
                index = value.IndexOf('-', 1);

            if (index != -1)
            {
                value = value.Substring(0, index);
            }

            var javaScriptTicks = long.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture);

            var utcDateTime = ConvertJavaScriptTicksToDateTime(javaScriptTicks);
            return utcDateTime;
        }

        private static DateTime ConvertJavaScriptTicksToDateTime(long javaScriptTicks)
        {
            var dateTime = new DateTime((javaScriptTicks * 10000) + InitialJavaScriptDateTicks, DateTimeKind.Utc);

            return dateTime;
        }
    }
}
