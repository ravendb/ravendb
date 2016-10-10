using System;
using Sparrow.Json;

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

            if (TryParseNumber(buffer, 2, out hours) == false)
                return false;

            if (TryParseNumber(buffer + 3, 2, out minutes) == false)
                return false;

            if (TryParseNumber(buffer + 6, 2, out seconds) == false)
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
                return Result.Failed;

            int year, month, day, hour, minute, second, fractions, offsetHour, offsetMinute;


            if (buffer[4] != '-' || buffer[7] != '-' || buffer[10] != 'T' ||
                buffer[13] != ':' || buffer[16] != ':' || buffer[16] != ':')
                return Result.Failed;

            if (TryParseNumber(buffer, 4, out year) == false)
                return Result.Failed;

            if (TryParseNumber(buffer + 5, 2, out month) == false)
                return Result.Failed;

            if (TryParseNumber(buffer + 8, 2, out day) == false)
                return Result.Failed;

            if (TryParseNumber(buffer + 11, 2, out hour) == false)
                return Result.Failed;

            if (TryParseNumber(buffer + 14, 2, out minute) == false)
                return Result.Failed;

            if (TryParseNumber(buffer + 17, 2, out second) == false)
                return Result.Failed;

            var kind = DateTimeKind.Unspecified;

            switch (len)
            {
                case 19://"yyyy'-'MM'-'dd'T'HH':'mm':'ss",
                    dt = new DateTime(year, month, day, hour, minute, second, kind);
                    return Result.DateTime;
                case 20: //"yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'",
                    if (buffer[19] != 'Z')
                        return Result.Failed;
                    kind = DateTimeKind.Utc;
                    goto case 19;
                case 23://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff",
                    if (buffer[19] != '.')
                        return Result.Failed;
                    if (TryParseNumber(buffer + 20, 3, out fractions) == false)
                        return Result.Failed;
                    dt = new DateTime(year, month, day, hour, minute, second, kind).AddTicks(fractions);
                    return Result.DateTime;
                case 24://"yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'",
                    if (buffer[23] != 'Z')
                        return Result.Failed;
                    kind = DateTimeKind.Utc;
                    goto case 23;
                case 27://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff"
                    if (buffer[19] != '.')
                        return Result.Failed;
                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        return Result.Failed;
                    dt = new DateTime(year, month, day, hour, minute, second, kind).AddTicks(fractions);
                    return Result.DateTime;
                case 28://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'Z'"
                    if (buffer[27] != 'Z')
                        return Result.Failed;
                    kind = DateTimeKind.Utc;
                    goto case 27;
                case 33://"yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff'+'dd':'dd"
                    if (buffer[19] != '.' || buffer[30] != ':' ||
                        (buffer[27] != '+' && buffer[27] != '-'))
                        return Result.Failed;

                    if (TryParseNumber(buffer + 20, 7, out fractions) == false)
                        return Result.Failed;
                    if (TryParseNumber(buffer + 28, 2, out offsetHour) == false)
                        return Result.Failed;
                    if (TryParseNumber(buffer + 31, 2, out offsetMinute) == false)
                        return Result.Failed;

                    var offset = new TimeSpan(offsetHour, offsetMinute, 0);
                    if (buffer[27] == '-')
                        offset = -offset;
                    dto = new DateTimeOffset(year, month, day, hour, minute, second,
                        offset).AddTicks(fractions);
                    return Result.DateTimeOffset;
                default:
                    return Result.Failed;
            }

        }

        private static bool TryParseNumber(byte* ptr, int size, out int val)
        {
            val = 0;
            for (int i = 0; i < size; i++)
            {
                val *= 10;
                if (ptr[i] < '0' || ptr[i] > '9')
                    return false;

                val += ptr[i] - '0';
            }
            return true;
        }
    }
}