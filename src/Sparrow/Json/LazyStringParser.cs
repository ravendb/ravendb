using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Utils;

namespace Sparrow.Json
{
    /// <summary>
    /// This class assumes that the data is in UTF8
    /// </summary>
    public unsafe class LazyStringParser
    {
        public enum Result
        {
            Failed,
            DateTime,
            DateTimeOffset
        }

        /*
        * Lookup tables that converts from a decimal character value to an integral
        * binary value, shifted by a decimal "shift" multiplier.
        * For all character values in the range '0'..'9', the table at those
        * index locations returns the actual decimal value shifted by the multiplier.
        * For all other values, the lookup table returns an invalid OOR value.
        */

        // Out-of-range flag value, larger than the largest value that can fit in
        // four decimal bytes (9999), but four of these added up together should
        // still not overflow.
        private const int OOR = 10000;

        private static readonly int* shift1;
        private static readonly int* shift10;
        private static readonly int* shift100;
        private static readonly int* shift1000;

        static LazyStringParser()
        {
            // PERF: The following code will build tables like this that will guarantee that the addition is outside of the
            //       proper range to detect failures.
            //    shift1 = new[] {
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 0-9
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, //  10
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, //  20
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, //  30
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR,   0,   1, //  40
            //      2,   3,   4,   5,   6,   7,   8,   9, OOR, OOR, //  50
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, //  60
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, //  70
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, //  80
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, //  90
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 100
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 110
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 120
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 130
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 140
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 150
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 160
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 170
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 180
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 190
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 200
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 210
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 220
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 230
            //    OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, OOR, // 240
            //    OOR, OOR, OOR, OOR, OOR, OOR // 250
            //};

            // PERF: will use native memory to avoid the overhead of the bound check which cannot be evicted otherwise.
            shift1 = (int*)NativeMemory.AllocateMemory(256 * sizeof(int));
            for (int i = 0; i < 256; i++)
            {
                if ((char)i >= '0' && (char)i <= '9')
                    shift1[i] = (int)(i - '0');
                else
                    shift1[i] = OOR;
            }

            shift10 = (int*)NativeMemory.AllocateMemory(256 * sizeof(int));
            for (int i = 0; i < 256; i++)
            {
                if ((char)i >= '0' && (char)i <= '9')
                    shift10[i] = 10 * (int)(i - '0');
                else
                    shift10[i] = OOR;
            }

            shift100 = (int*)NativeMemory.AllocateMemory(256 * sizeof(int));
            for (int i = 0; i < 256; i++)
            {
                if ((char)i >= '0' && (char)i <= '9')
                    shift100[i] = 100 * (int)(i - '0');
                else
                    shift100[i] = OOR;
            }

            shift1000 = (int*)NativeMemory.AllocateMemory(256 * sizeof(int));
            for (int i = 0; i < 256; i++)
            {
                if ((char)i >= '0' && (char)i <= '9')
                    shift1000[i] = 1000 * (int)(i - '0');
                else
                    shift1000[i] = OOR;
            }
        }

        // Number of 100ns ticks per time unit
        private const long TicksPerMillisecond = 10000;
        private const long TicksPerSecond = TicksPerMillisecond * 1000;
        private const long TicksPerMinute = TicksPerSecond * 60;
        private const long TicksPerHour = TicksPerMinute * 60;
        private const long TicksPerDay = TicksPerHour * 24;

        private static readonly int[] _daysToMonth365 = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 };
        private static readonly int[] _daysToMonth366 = { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366 };

        private const long MaxSeconds = long.MaxValue / TicksPerSecond;
        private const long MinSeconds = long.MinValue / TicksPerSecond;

        // Returns the tick count corresponding to the given year, month, and day.
        // Will check the if the parameters are valid.
        private static long DateToTicks(int year, int month, int day, int hour, int minute, int second, int fraction)
        {
            long dateTicks = 0;
            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12)
            {
                int[] days = DateTime.IsLeapYear(year) ? _daysToMonth366 : _daysToMonth365;
                if (day >= 1 && day <= days[month] - days[month - 1])
                {
                    int y = year - 1;
                    int n = y * 365 + y / 4 - y / 100 + y / 400 + days[month - 1] + day - 1;
                    dateTicks = n * TicksPerDay;
                }
            }

            // totalSeconds is bounded by 2^31 * 2^12 + 2^31 * 2^8 + 2^31,
            // which is less than 2^44, meaning we won't overflow totalSeconds.
            long totalSeconds = (long)hour * 3600 + (long)minute * 60 + (long)second;
            if (totalSeconds > MaxSeconds || totalSeconds < MinSeconds)
                throw new ArgumentOutOfRangeException(null, "Overflow_TimeSpanTooLong");

            return dateTicks + totalSeconds * TicksPerSecond + fraction;
        }

        public static bool TryParseTimeSpan(byte* buffer, int len, out TimeSpan ts)
        {
            ts = default(TimeSpan);

            if (len < 8)
                return false;

            bool negate = false;
            if (buffer[0] == '-')
            {
                negate = true;
                buffer++;
                len--;
            }

            int indexOfDays = -1, indexOfMilliseconds = -1;

            for (int i = 0; i < len; i++)
            {
                if (buffer[i] == ':')
                    break;
                if (buffer[i] == '.')
                {
                    indexOfDays = i;
                    break;
                }
            }

            if (len < indexOfDays + 1 + 8)
                return false;

            for (int i = indexOfDays + 1 + 8; i < len; i++)
            {
                if (buffer[i] == '.')
                {
                    indexOfMilliseconds = i;
                    break;
                }
            }

            if (indexOfMilliseconds == -1) // if we have ms then it will be validated below
            {
                if (indexOfDays == -1)
                {
                    if (len > 8)
                        return false;
                }
                else
                {
                    if (len - indexOfDays - 1 - 8 > 0)
                        return false;
                }
            }

            int days = 0, hours, minutes, seconds, ticks = 0;

            if (indexOfDays != -1)
            {
                if (TryParseNumber(buffer, indexOfDays, out days) == false)
                    return false;
                buffer += indexOfDays + 1;
            }

            if (buffer[2] != ':' || buffer[5] != ':')
                return false;

            if (TryParseNumber2(buffer, 0, out hours) == false)
                return false;

            if (TryParseNumber2(buffer, 3, out minutes) == false)
                return false;

            if (TryParseNumber2(buffer, 6, out seconds) == false)
                return false;

            if (indexOfMilliseconds != -1)
            {
                var remainingLen = len - indexOfMilliseconds - 1;
                if (remainingLen > 7)
                    return false;
                if (TryParseNumber(buffer + 9, remainingLen, out ticks) == false)
                    return false;

                for (int i = remainingLen; i < 7; i++)
                {
                    ticks *= 10;
                }
            }
            
            if (negate)
            {
                ts = new TimeSpan(days*-1, hours * -1, minutes * -1, seconds * -1).Add(TimeSpan.FromTicks(ticks * -1));
            }
            else
            {
                ts = new TimeSpan(days, hours, minutes, seconds).Add(TimeSpan.FromTicks(ticks));
            }                
            return true;
        }
        
        public static bool TryParseTimeSpan(char* buffer, int len, out TimeSpan ts)
        {
            ts = default(TimeSpan);

            if (len < 8)
                return false;

            bool negate = false;
            if (buffer[0] == '-')
            {
                negate = true;
                buffer++;
                len--;
            }

            int indexOfDays = -1, indexOfMilliseconds = -1;

            for (int i = 0; i < len; i++)
            {
                if (buffer[i] == ':')
                    break;
                if (buffer[i] == '.')
                {
                    indexOfDays = i;
                    break;
                }
            }

            if (len < indexOfDays + 1 + 8)
                return false;

            for (int i = indexOfDays + 1 + 8; i < len; i++)
            {
                if (buffer[i] == '.')
                {
                    indexOfMilliseconds = i;
                    break;
                }
            }

            if (indexOfMilliseconds == -1) // if we have ms then it will be validated below
            {
                if (indexOfDays == -1)
                {
                    if (len > 8)
                        return false;
                }
                else
                {
                    if (len - indexOfDays - 1 - 8 > 0)
                        return false;
                }
            }

            int days = 0, ticks = 0;

            if (indexOfDays != -1)
            {
                if (TryParseNumber(buffer, indexOfDays, out days) == false)
                    return false;
                buffer += indexOfDays + 1;
            }

            if (buffer[2] != ':' || buffer[5] != ':')
                return false;

            if (TryParseNumber2(buffer, 0, out int hours) == false)
                return false;

            if (TryParseNumber2(buffer, 3, out int minutes) == false)
                return false;

            if (TryParseNumber2(buffer, 6, out int seconds) == false)
                return false;

            if (indexOfMilliseconds != -1)
            {
                var remainingLen = len - indexOfMilliseconds - 1;
                if (remainingLen > 7)
                    return false;
                if (TryParseNumber(buffer + 9, remainingLen, out ticks) == false)
                    return false;

                for (int i = remainingLen; i < 7; i++)
                {
                    ticks *= 10;
                }
            }

            
            if (negate)
            {
                ts = new TimeSpan(days*-1, hours * -1, minutes * -1, seconds * -1).Add(TimeSpan.FromTicks(ticks * -1));
            }
            else
            {
                ts = new TimeSpan(days, hours, minutes, seconds).Add(TimeSpan.FromTicks(ticks));
            }                
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Result TryParseDateTime(char* buffer, int len, out DateTime dt, out DateTimeOffset dto)
        {
            // PERF: We want this part of the code to be embedded into the caller code instead. 
            if (len < 19 || len > 33)
                goto Failed;

            if (buffer[4] != '-' || buffer[7] != '-' || buffer[10] != 'T' ||
                buffer[13] != ':' || buffer[16] != ':' || buffer[16] != ':')
                goto Failed;

            return TryParseDateTimeInternal(buffer, len, out dt, out dto);

            Failed:
            dt = default(DateTime);
            dto = default(DateTimeOffset);
            return Result.Failed;
        }

        public static Result TryParseDateTimeInternal(char* buffer, int len, out DateTime dt, out DateTimeOffset dto)
        {
            if (TryParseNumber4(buffer, 0, out int year) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 5, out int month) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 8, out int day) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 11, out int hour) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 14, out int minute) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 17, out int second) == false)
                goto Failed;

            var kind = DateTimeKind.Unspecified;

            Result result = Result.DateTime;

            int fractions = 0;
            switch (len)
            {
                case 20: //"yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'",
                    if (buffer[19] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 19;
                case 19://"yyyy'-'MM'-'dd'T'HH':'mm':'ss",                    
                    goto Finished_DT;
                case 24://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'",
                    if (buffer[23] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 23;
                case 23://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff",
                    if (buffer[19] != '.')
                        goto Failed;
                    if (TryParseNumber3(buffer, 20, out fractions) == false)
                        goto Failed;
                    goto Finished_DT;
                case 28://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'Z'"
                    if (buffer[27] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 27;
                case 27://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"
                    if (buffer[19] != '.')
                        goto Failed;
                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        goto Failed;
                    goto Finished_DT;
                case 33://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'+'dd':'dd"
                    if (buffer[19] != '.' || buffer[30] != ':' || (buffer[27] != '+' && buffer[27] != '-'))
                        goto Failed;

                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        goto Failed;

                    if (TryParseNumber2(buffer, 28, out int offsetHour) == false)
                        goto Failed;
                    if (TryParseNumber2(buffer, 31, out int offsetMinute) == false)
                        goto Failed;

                    var offset = new TimeSpan(offsetHour, offsetMinute, 0);
                    if (buffer[27] == '-')
                        offset = -offset;

                    dt = default(DateTime);
                    dto = new DateTimeOffset(DateToTicks(year, month, day, hour, minute, second, fractions), offset);
                    result = Result.DateTimeOffset;
                    goto Finished;
            }

            Finished_DT:
            dt = new DateTime(DateToTicks(year, month, day, hour, minute, second, fractions), kind);
            dto = default(DateTimeOffset);

            Finished:
            return result;

            Failed:
            dt = default(DateTime);
            dto = default(DateTimeOffset);
            result = Result.Failed;
            goto Finished;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Result TryParseDateTime(byte* buffer, int len, out DateTime dt, out DateTimeOffset dto)
        {
            // PERF: We want this part of the code to be embedded into the caller code instead. 
            if (len < 19 || len > 33)
                goto Failed;

            if (buffer[4] != '-' || buffer[7] != '-' || buffer[10] != 'T' ||
                buffer[13] != ':' || buffer[16] != ':' || buffer[16] != ':')
                goto Failed;

            return TryParseDateTimeInternal(buffer, len, out dt, out dto);

            Failed:
            dt = default(DateTime);
            dto = default(DateTimeOffset);
            return Result.Failed;
        }

        private static Result TryParseDateTimeInternal(byte* buffer, int len, out DateTime dt, out DateTimeOffset dto)
        {
            if (TryParseNumber4(buffer, 0, out int year) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 5, out int month) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 8, out int day) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 11, out int hour) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 14, out int minute) == false)
                goto Failed;

            if (TryParseNumber2(buffer, 17, out int second) == false)
                goto Failed;
            
            var kind = DateTimeKind.Unspecified;

            Result result = Result.DateTime;

            int fractions = 0;
            switch (len)
            {
                case 20: //"yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'",
                    if (buffer[19] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 19;
                case 19://"yyyy'-'MM'-'dd'T'HH':'mm':'ss",                    
                    goto Finished_DT;
                case 24://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'",
                    if (buffer[23] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 23;
                case 23://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff",
                    if (buffer[19] != '.')
                        goto Failed;
                    if (TryParseNumber3(buffer, 20, out fractions) == false)
                        goto Failed;
                    goto Finished_DT;
                case 28://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'Z'"
                    if (buffer[27] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 27;
                case 27://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"
                    if (buffer[19] != '.')
                        goto Failed;
                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        goto Failed;
                    goto Finished_DT;
                case 33://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'+'dd':'dd"
                    if (buffer[19] != '.' || buffer[30] != ':' || (buffer[27] != '+' && buffer[27] != '-'))
                        goto Failed;

                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        goto Failed;

                    if (TryParseNumber2(buffer, 28, out int offsetHour) == false)
                        goto Failed;
                    if (TryParseNumber2(buffer, 31, out int offsetMinute) == false)
                        goto Failed;

                    var offset = new TimeSpan(offsetHour, offsetMinute, 0);
                    if (buffer[27] == '-')
                        offset = -offset;

                    dt = default(DateTime);                    
                    dto = new DateTimeOffset(DateToTicks(year, month, day, hour, minute, second, fractions), offset);
                    result = Result.DateTimeOffset;
                    goto Finished;
            }

            Finished_DT:
            dt = new DateTime(DateToTicks(year, month, day, hour, minute, second, fractions), kind);
            dto = default(DateTimeOffset);

            Finished:                            
            return result;

            Failed:
            dt = default(DateTime);
            dto = default(DateTimeOffset);
            result = Result.Failed;
            goto Finished;
        }

        private static bool TryParseNumber(byte* ptr, int size, out int val)
        {
            val = 0;
            for (int i = 0; i < size; i++)
            {
                val *= 10;
                if (ptr[i] < '0' || ptr[i] > '9')
                    goto Failed;

                val += ptr[i] - '0';
            }
            return true;

            Failed:
            val = 0;
            return false;
        }

        private static bool TryParseNumber(char* ptr, int size, out int val)
        {
            val = 0;
            for (int i = 0; i < size; i++)
            {
                val *= 10;
                if (ptr[i] < '0' || ptr[i] > '9')
                    goto Failed;

                val += ptr[i] - '0';
            }
            return true;

            Failed:
            val = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber2(byte* ptr, int offset, out int val)
        {
            // PERF: We use ptr + offset to ensure we can hijack the chip front-end to do the arithmetic instead of relying on the backend
            int r0 = shift10[(ptr + offset)[0]];
            int r1 = shift1[(ptr + offset)[1]];
            val = r0 + r1;

            return val < OOR;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber2(char* ptr, int offset, out int val)
        {
            // We are looking for non-ascii code points (if we have any non ascii bit enabled, we will return false)
            bool isAscii = ((ptr + offset)[0] | (ptr + offset)[1]) < 256;

            // PERF: We use ptr + offset to ensure we can hijack the chip front-end to do the arithmetic instead of relying on the backend
            int r0 = shift10[(ptr + offset)[0]];
            int r1 = shift1[(ptr + offset)[1]];
            val = r0 + r1;

            return val < OOR && isAscii;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber3(byte* ptr, int offset, out int val)
        {
            // PERF: We use ptr + offset to ensure we can hijack the chip front-end to do the arithmetic instead of relying on the backend
            int r0 = shift100[(ptr + offset)[0]];
            int r1 = shift10[(ptr + offset)[1]];
            int r2 = shift1[(ptr + offset)[2]];
            val = r0 + r1 + r2;

            return val < OOR;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber3(char* ptr, int offset, out int val)
        {
            // We are looking for non-ascii code points (if we have any non ascii bit enabled, we will return false)
            bool isAscii = ((ptr + offset)[0] | (ptr + offset)[1] | (ptr + offset)[2]) < 256;

            // PERF: We use ptr + offset to ensure we can hijack the chip front-end to do the arithmetic instead of relying on the backend
            int r0 = shift100[(ptr + offset)[0]];
            int r1 = shift10[(ptr + offset)[1]];
            int r2 = shift1[(ptr + offset)[2]];
            val = r0 + r1 + r2;

            return val < OOR && isAscii;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber4(byte* ptr, int offset, out int val)
        {
            // PERF: We use ptr + offset to ensure we can hijack the chip front-end to do the arithmetic instead of relying on the backend
            int r0 = shift1000[(ptr + offset)[0]];
            int r1 = shift100[(ptr + offset)[1]];
            int r2 = shift10[(ptr + offset)[2]];
            int r3 = shift1[(ptr + offset)[3]];
            val = r0 + r1 + r2 + r3;

            return val < OOR;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber4(char* ptr, int offset, out int val)
        {
            // We are looking for non-ascii code points (if we have any non ascii bit enabled, we will return false)
            bool isAscii = ((ptr + offset)[0] | (ptr + offset)[1] | (ptr + offset)[2] | (ptr + offset)[3]) < 256;

            // PERF: We use ptr + offset to ensure we can hijack the chip front-end to do the arithmetic instead of relying on the backend
            int r0 = shift1000[(ptr + offset)[0]];
            int r1 = shift100[(ptr + offset)[1]];
            int r2 = shift10[(ptr + offset)[2]];
            int r3 = shift1[(ptr + offset)[3]];
            val = r0 + r1 + r2 + r3;

            return val < OOR && isAscii;
        }
    }
}
