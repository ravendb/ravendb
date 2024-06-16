/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Lucene.Net.Support
{
    /// <summary>
    /// A simple class for number conversions.
    /// </summary>
    public class Number
    {
        /// <summary>
        /// Min radix value.
        /// </summary>
        public const int MIN_RADIX = 2;
        /// <summary>
        /// Max radix value.
        /// </summary>
        public const int MAX_RADIX = 36;

        private const System.String digits = "0123456789abcdefghijklmnopqrstuvwxyz";


        /// <summary>
        /// Converts a number to System.String.
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        public static System.String ToString(long number)
        {
            System.Text.StringBuilder s = new System.Text.StringBuilder();

            if (number == 0)
            {
                s.Append("0");
            }
            else
            {
                if (number < 0)
                {
                    s.Append("-");
                    number = -number;
                }

                while (number > 0)
                {
                    char c = digits[(int)number % 36];
                    s.Insert(0, c);
                    number = number / 36;
                }
            }

            return s.ToString();
        }


        /// <summary>
        /// Converts a number to System.String.
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public static System.String ToString(float f)
        {
            if (((float)(int)f) == f)
            {
                return ((int)f).ToString() + ".0";
            }
            else
            {
                return f.ToString(NumberFormatInfo.InvariantInfo);
            }
        }

        /// <summary>
        /// Converts a number to System.String in the specified radix.
        /// </summary>
        /// <param name="i">A number to be converted.</param>
        /// <param name="radix">A radix.</param>
        /// <returns>A System.String representation of the number in the specified redix.</returns>
        public static System.String ToString(long i, int radix)
        {
            if (radix < MIN_RADIX || radix > MAX_RADIX)
                radix = 10;

            char[] buf = new char[65];
            int charPos = 64;
            bool negative = (i < 0);

            if (!negative)
            {
                i = -i;
            }

            while (i <= -radix)
            {
                buf[charPos--] = digits[(int)(-(i % radix))];
                i = i / radix;
            }
            buf[charPos] = digits[(int)(-i)];

            if (negative)
            {
                buf[--charPos] = '-';
            }

            return new System.String(buf, charPos, (65 - charPos));
        }

        /// <summary>
        /// Parses a number in the specified radix.
        /// </summary>
        /// <param name="s">An input System.String.</param>
        /// <param name="radix">A radix.</param>
        /// <returns>The parsed number in the specified radix.</returns>
        public static long Parse(System.String s, int radix)
        {
            if (s == null)
            {
                throw new ArgumentException("null");
            }

            if (radix < MIN_RADIX)
            {
                throw new NotSupportedException("radix " + radix +
                                                " less than Number.MIN_RADIX");
            }
            if (radix > MAX_RADIX)
            {
                throw new NotSupportedException("radix " + radix +
                                                " greater than Number.MAX_RADIX");
            }

            long result = 0;
            long mult = 1;

            s = s.ToLower();

            for (int i = s.Length - 1; i >= 0; i--)
            {
                int weight = digits.IndexOf(s[i]);
                if (weight == -1)
                    throw new FormatException("Invalid number for the specified radix");

                result += (weight * mult);
                mult *= radix;
            }

            return result;
        }

        /// <summary>
        /// Performs an unsigned bitwise right shift with the specified number
        /// </summary>
        /// <param name="number">Number to operate on</param>
        /// <param name="bits">Ammount of bits to shift</param>
        /// <returns>The resulting number from the shift operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int URShift(int number, int bits)
        {
            return (int)(((uint)number) >> bits);
        }


        /// <summary>
        /// Performs an unsigned bitwise right shift with the specified number
        /// </summary>
        /// <param name="number">Number to operate on</param>
        /// <param name="bits">Ammount of bits to shift</param>
        /// <returns>The resulting number from the shift operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long URShift(long number, int bits)
        {
            return (long)(((ulong)number) >> bits);
        }


        /// <summary>
        /// Returns the index of the first bit that is set to true that occurs 
        /// on or after the specified starting index. If no such bit exists 
        /// then -1 is returned.
        /// </summary>
        /// <param name="bits">The BitArray object.</param>
        /// <param name="fromIndex">The index to start checking from (inclusive).</param>
        /// <returns>The index of the next set bit.</returns>
        public static int NextSetBit(System.Collections.BitArray bits, int fromIndex)
        {
            for (int i = fromIndex; i < bits.Length; i++)
            {
                if (bits[i] == true)
                {
                    return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// Converts a System.String number to long.
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static long ToInt64(System.String s)
        {
            long number = 0;
            long factor;

            // handle negative number
            if (s.StartsWith("-"))
            {
                s = s.Substring(1);
                factor = -1;
            }
            else
            {
                factor = 1;
            }

            // generate number
            for (int i = s.Length - 1; i > -1; i--)
            {
                int n = digits.IndexOf(s[i]);

                // not supporting fractional or scientific notations
                if (n < 0)
                    throw new System.ArgumentException("Invalid or unsupported character in number: " + s[i]);

                number += (n * factor);
                factor *= 36;
            }

            return number;
        }
    }
}
