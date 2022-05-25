using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    internal static class BasicComparers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareAscending(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
        {
            return x.SequenceCompareTo(y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsSpace(byte c)
        {
            return c == ' ';
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsSpace(char c)
        {
            return c == ' ';
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsDigit(byte c)
        {
            return c >= '0' && c <= '9';
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsDigit(char c)
        {
            return c >= '0' && c <= '9';
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte ToUpper(byte c)
        {
            if (c >= 'a' && c <= 'z')
                return (byte)(c - ('a' - 'A'));

            return c;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static char ToUpper(char c)
        {
            return char.ToUpperInvariant(c);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsSameLetterDifferentCase(byte a, byte b)
        {
            return (a >= 'A' && a <= 'z') && (b >= 'A' && b <= 'z') && (a - ('a' - 'A') == b || a + ('a' - 'A') == b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsSameLetterDifferentCase(char a, char b)
        {
            return (a >= 'A' && a <= 'z') && (b >= 'A' && b <= 'z') && (a - ('a' - 'A') == b || a + ('a' - 'A') == b);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsLetter(byte c)
        {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsLetter(char c)
        {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareAlphanumericAscending(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
        {
            if (a.Length == 0)
                return b.Length == 0 ? 0 : -1;
            if (a.Length == 0)
                return 1;

            // Natural order string sorting ported from Martin Pool's C implementation as a reference. 
            //   strnatcmp.c -- Perform 'natural order' comparisons of strings in C.
            // https://github.com/sourcefrog/natsort               

            int ai = 0, bi = 0;
            while (true)
            {
                byte ca = ai < a.Length ? a[ai] : (byte)0;
                byte cb = bi < b.Length ? b[bi] : (byte)0;

                // We need to check if it is ASCII or not and act accordingly.
                if ((ca & 0b1000_0000) != 0 || (cb & 0b1000_0000) != 0)
                    goto NotAscii;

                /* skip over leading spaces or zeros */
                while (IsSpace(ca))
                    ca = a[++ai];

                while (IsSpace(cb))
                    cb = b[++bi];

                /* process run of digits */
                if (IsDigit(ca) && IsDigit(cb))
                {
                    bool fractional = (ca == '0' || cb == '0');
                    if (fractional)
                    {
                        int result = CompareLeft(a.Slice(ai), b.Slice(bi));
                        if (result != 0)
                            return result;
                    }
                    else
                    {
                        int result = CompareRight(a.Slice(ai), b.Slice(bi));
                        if (result != 0)
                            return result;
                    }
                }

                if (ca == 0 && cb == 0)
                {
                    /* The strings compare the same.  Perhaps the caller
                           will want to call strcmp to break the tie. */
                    return 0;
                }

                // If both are letters... 
                if (IsLetter(ca) && IsLetter(cb))
                {
                    if (IsSameLetterDifferentCase(ca, cb))
                    {
                        return -(ca - cb);
                    }

                    var cca = ToUpper(ca);
                    var ccb = ToUpper(cb);
                    if (cca != ccb)
                    {
                        return cca - ccb;
                    }
                }
                else if (ca != cb)
                {
                    return ca - cb;
                }

                ++ai;
                ++bi;
            }

            NotAscii:
            return CompareAlphanumericAscendingUtf8(a.Slice(ai), b.Slice(bi));
        }

        private static int CompareAlphanumericAscendingUtf8(ReadOnlySpan<byte> byteA, ReadOnlySpan<byte> byteB)
        {
            var auxiliarMemory = ArrayPool<char>.Shared.Rent(byteA.Length + byteB.Length);
            var a = auxiliarMemory.AsSpan().Slice(0, byteA.Length);
            var b = auxiliarMemory.AsSpan().Slice(byteA.Length, byteB.Length);

            var aLength = Encoding.UTF8.GetChars(byteA, a);
            var bLength = Encoding.UTF8.GetChars(byteB, b);

            a.Slice(0, aLength);
            b.Slice(0, bLength);

            int result = 0;
            int ai = 0, bi = 0;
            while (true)
            {
                char ca = ai < a.Length ? a[ai] : (char)0;
                char cb = bi < b.Length ? b[bi] : (char)0;

                /* skip over leading spaces or zeros */
                while (IsSpace(ca))
                    ca = a[++ai];

                while (IsSpace(cb))
                    cb = b[++bi];

                /* process run of digits */
                if (IsDigit(ca) && IsDigit(cb))
                {
                    bool fractional = (ca == '0' || cb == '0');
                    if (fractional)
                    {
                        result = CompareLeft(a.Slice(ai), b.Slice(bi));
                        if (result != 0)
                            goto End;
                    }
                    else
                    {
                        result = CompareRight(a.Slice(ai), b.Slice(bi));
                        if (result != 0)
                            goto End;
                    }
                }

                if (ca == 0 && cb == 0)
                {
                    /* The strings compare the same.  Perhaps the caller
                           will want to call strcmp to break the tie. */
                    result = 0;
                    goto End;
                }

                // If both are letters... 
                if (IsLetter(ca) && IsLetter(cb))
                {
                    if (IsSameLetterDifferentCase(ca, cb))
                    {
                        return -(ca - cb);
                    }

                    var cca = ToUpper(ca);
                    var ccb = ToUpper(cb);
                    if (cca != ccb)
                    {
                        return cca - ccb;
                    }
                }
                else if (ca != cb)
                {
                    return ca - cb;
                }

                ++ai;
                ++bi;
            }


            End:
            ArrayPool<char>.Shared.Return(auxiliarMemory);
            return result;
        }

        private static int CompareRight(ReadOnlySpan<byte> ax, ReadOnlySpan<byte> bx)
        {
            int bias = 0;
            int a = 0;
            int b = 0;

            /* The longest run of digits wins.  That aside, the greatest
               value wins, but we can't know that it will until we've scanned
               both numbers to know that they have the same magnitude, so we
               remember it in BIAS. */
            for (;; a++, b++)
            {
                byte av = a < ax.Length ? ax[a] : (byte)0;
                byte bv = b < bx.Length ? bx[b] : (byte)0;

                if (!IsDigit(av) && !IsDigit(bv))
                    return bias;
                if (!IsDigit(av))
                    return -1;
                if (!IsDigit(bv))
                    return +1;
                if (av < bv)
                {
                    if (bias != 0)
                        bias = -1;
                }
                else if (av > bv)
                {
                    if (bias != 0)
                        bias = +1;
                }
                else if (a == 0 || b == 0)
                    return bias;
            }
        }

        private static int CompareRight(ReadOnlySpan<char> ax, ReadOnlySpan<char> bx)
        {
            int bias = 0;
            int a = 0;
            int b = 0;

            /* The longest run of digits wins.  That aside, the greatest
               value wins, but we can't know that it will until we've scanned
               both numbers to know that they have the same magnitude, so we
               remember it in BIAS. */
            for (;; a++, b++)
            {
                char av = a < ax.Length ? ax[a] : (char)0;
                char bv = b < bx.Length ? bx[b] : (char)0;

                if (!IsDigit(av) && !IsDigit(bv))
                    return bias;
                if (!IsDigit(av))
                    return -1;
                if (!IsDigit(bv))
                    return +1;
                if (av < bv)
                {
                    if (bias != 0)
                        bias = -1;
                }
                else if (av > bv)
                {
                    if (bias != 0)
                        bias = +1;
                }
                else if (a == 0 || b == 0)
                    return bias;
            }
        }

        private static int CompareLeft(ReadOnlySpan<byte> ax, ReadOnlySpan<byte> bx)
        {
            int a = 0;
            int b = 0;

            /* Compare two left-aligned numbers: the first to have a
               different value wins. */
            for (;; a++, b++)
            {
                byte av = a < ax.Length ? ax[a] : (byte)0;
                byte bv = b < bx.Length ? bx[b] : (byte)0;

                if (!IsDigit(av) && !IsDigit(bv))
                    return 0;
                if (!IsDigit(av))
                    return -1;
                if (!IsDigit(bv))
                    return +1;
                if (av < bv)
                    return -1;
                if (av > bv)
                    return +1;
            }
        }

        private static int CompareLeft(ReadOnlySpan<char> ax, ReadOnlySpan<char> bx)
        {
            int a = 0;
            int b = 0;

            /* Compare two left-aligned numbers: the first to have a
               different value wins. */
            for (;; a++, b++)
            {
                char av = a < ax.Length ? ax[a] : (char)0;
                char bv = b < bx.Length ? bx[b] : (char)0;

                if (!IsDigit(av) && !IsDigit(bv))
                    return 0;
                if (!IsDigit(av))
                    return -1;
                if (!IsDigit(bv))
                    return +1;
                if (av < bv)
                    return -1;
                if (av > bv)
                    return +1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareAscending<T>(T x, T y)
        {
            if (typeof(T) == typeof(long))
            {
                return Math.Sign((long)(object)x - (long)(object)y);
            }
            else if (typeof(T) == typeof(double))
            {
                return Math.Sign((double)(object)x - (double)(object)y);
            }

            throw new NotSupportedException("Not supported");
        }
    }
}
