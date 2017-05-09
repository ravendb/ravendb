using System;
using System.Runtime.CompilerServices;

namespace Raven.Server.Utils
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

            if (TryParseNumber2(buffer, out hours) == false)
                return false;

            if (TryParseNumber2(buffer + 3, out minutes) == false)
                return false;

            if (TryParseNumber2(buffer + 6, out seconds) == false)
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

            ts = new TimeSpan(days, hours, minutes, seconds).Add(TimeSpan.FromTicks(ticks));
            if (negate)
                ts = -ts;
            return true;
        }

        public static Result TryParseDateTime(byte* buffer, int len, out DateTime dt, out DateTimeOffset dto)
        {
            dt = default(DateTime);
            dto = default(DateTimeOffset);

            if (len < 19)
                goto Failed;

            int year, month, day, hour, minute, second, fractions;


            if (buffer[4] != '-' || buffer[7] != '-' || buffer[10] != 'T' ||
                buffer[13] != ':' || buffer[16] != ':' || buffer[16] != ':')
                goto Failed;

            if (TryParseNumber4(buffer, out year) == false)
                goto Failed;

            if (TryParseNumber2(buffer + 5, out month) == false)
                goto Failed;

            if (TryParseNumber2(buffer + 8, out day) == false)
                goto Failed;

            if (TryParseNumber2(buffer + 11, out hour) == false)
                goto Failed;

            if (TryParseNumber2(buffer + 14, out minute) == false)
                goto Failed;

            if (TryParseNumber2(buffer + 17, out second) == false)
                goto Failed;

            var kind = DateTimeKind.Unspecified;

            switch (len)
            {
                case 19://"yyyy'-'MM'-'dd'T'HH':'mm':'ss",
                    dt = new DateTime(year, month, day, hour, minute, second, kind);
                    return Result.DateTime;
                case 20: //"yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'",
                    if (buffer[19] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 19;
                case 23://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff",
                    if (buffer[19] != '.')
                        goto Failed;
                    if (TryParseNumber3(buffer + 20, out fractions) == false)
                        goto Failed;
                    dt = new DateTime(year, month, day, hour, minute, second, kind).AddTicks(fractions);
                    return Result.DateTime;
                case 24://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'",
                    if (buffer[23] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 23;
                case 27://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"
                    if (buffer[19] != '.')
                        goto Failed;
                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        goto Failed;
                    dt = new DateTime(year, month, day, hour, minute, second, kind).AddTicks(fractions);
                    return Result.DateTime;
                case 28://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'Z'"
                    if (buffer[27] != 'Z')
                        goto Failed;
                    kind = DateTimeKind.Utc;
                    goto case 27;
                case 33://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'+'dd':'dd"
                    if (buffer[19] != '.' || buffer[30] != ':' || (buffer[27] != '+' && buffer[27] != '-'))
                        goto Failed;

                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        goto Failed;

                    if (TryParseNumber2(buffer + 28, out int offsetHour) == false)
                        goto Failed;
                    if (TryParseNumber2(buffer + 31, out int offsetMinute) == false)
                        goto Failed;

                    var offset = new TimeSpan(offsetHour, offsetMinute, 0);
                    if (buffer[27] == '-')
                        offset = -offset;
                    dto = new DateTimeOffset(year, month, day, hour, minute, second, offset).AddTicks(fractions);
                    return Result.DateTimeOffset;
            }        

            Failed: return Result.Failed;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber2(byte* ptr, out int val)
        {
            if (ptr[0] < '0' || ptr[0] > '9' || ptr[1] < '0' || ptr[1] > '9')
                goto Failed;

            val = (ptr[0] - '0') * 10 + (ptr[1] - '0');
            return true;

            Failed:
            val = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber3(byte* ptr, out int val)
        {
            if (ptr[0] < '0' || ptr[0] > '9' || ptr[1] < '0' || ptr[1] > '9' || ptr[2] < '0' || ptr[2] > '9')
                goto Failed;

            val = (ptr[0] - '0') * 100 + (ptr[1] - '0') * 10 + (ptr[2] - '0');
            return true;

            Failed:
            val = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryParseNumber4(byte* ptr, out int val)
        {
            if (ptr[0] < '0' || ptr[0] > '9' || ptr[1] < '0' || ptr[1] > '9' || ptr[2] < '0' || ptr[2] > '9' || ptr[3] < '0' || ptr[3] > '9')
                goto Failed;

            val = (ptr[0] - '0') * 1000 + (ptr[1] - '0') * 100 + (ptr[2] - '0') * 10 + (ptr[3] - '0');
            return true;

            Failed:
            val = 0;
            return false;
        }
    }
}