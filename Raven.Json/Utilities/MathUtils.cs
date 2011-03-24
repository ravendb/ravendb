using System;

namespace Raven.Json.Utilities
{
    internal class MathUtils
    {
        public static int IntLength(int i)
        {
            if (i < 0)
                throw new ArgumentOutOfRangeException();

            if (i == 0)
                return 1;

            return (int)Math.Floor(Math.Log10(i)) + 1;
        }

        public static int HexToInt(char h)
        {
            if ((h >= '0') && (h <= '9'))
            {
                return (h - '0');
            }
            if ((h >= 'a') && (h <= 'f'))
            {
                return ((h - 'a') + 10);
            }
            if ((h >= 'A') && (h <= 'F'))
            {
                return ((h - 'A') + 10);
            }
            return -1;
        }

        public static char IntToHex(int n)
        {
            if (n <= 9)
            {
                return (char)(n + 48);
            }
            return (char)((n - 10) + 97);
        }

        public static int GetDecimalPlaces(double value)
        {
            // increasing max decimal places above 10 produces weirdness
            int maxDecimalPlaces = 10;
            double threshold = Math.Pow(0.1d, maxDecimalPlaces);

            if (value == 0.0)
                return 0;
            int decimalPlaces = 0;
            while (value - Math.Floor(value) > threshold && decimalPlaces < maxDecimalPlaces)
            {
                value *= 10.0;
                decimalPlaces++;
            }
            return decimalPlaces;
        }

        public static int? Min(int? val1, int? val2)
        {
            if (val1 == null)
                return val2;
            if (val2 == null)
                return val1;

            return Math.Min(val1.Value, val2.Value);
        }

        public static int? Max(int? val1, int? val2)
        {
            if (val1 == null)
                return val2;
            if (val2 == null)
                return val1;

            return Math.Max(val1.Value, val2.Value);
        }

        public static double? Min(double? val1, double? val2)
        {
            if (val1 == null)
                return val2;
            if (val2 == null)
                return val1;

            return Math.Min(val1.Value, val2.Value);
        }

        public static double? Max(double? val1, double? val2)
        {
            if (val1 == null)
                return val2;
            if (val2 == null)
                return val1;

            return Math.Max(val1.Value, val2.Value);
        }

        public static bool ApproxEquals(double d1, double d2)
        {
            // are values equal to within 6 (or so) digits of precision?
            return Math.Abs(d1 - d2) < (Math.Abs(d1) * 1e-6);
        }
    }

}
