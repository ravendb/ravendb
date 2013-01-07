//-----------------------------------------------------------------------
// <copyright file="DataGenerator.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace BasicTest
{
    using System;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;

    /// <summary>
    /// Methods to generate random column data.
    /// </summary>
    internal static class DataGenerator
    {
        /// <summary>
        /// Gets random data for a column.
        /// </summary>
        /// <param name="coltyp">The type of the column.</param>
        /// <param name="cp">
        /// The code page of the column. Only used for text columns.
        /// </param>
        /// <param name="rand">Random number generator.</param>
        /// <returns>
        /// Random data for the column.
        /// </returns>
        public static byte[] GetRandomColumnData(JET_coltyp coltyp, JET_CP cp, Random rand)
        {
            byte[] data;

            switch (coltyp)
            {
                case JET_coltyp.Bit:
                    data = new byte[1];
                    data[0] = (0 == rand.Next(2)) ? checked((byte)0x0) : checked((byte)0xFF);
                    break;
                case JET_coltyp.UnsignedByte:
                    data = new byte[1];
                    rand.NextBytes(data);
                    break;
                case JET_coltyp.Short:
                    data = new byte[2];
                    rand.NextBytes(data);
                    break;
                case JET_coltyp.Long:
                    data = new byte[4];
                    rand.NextBytes(data);
                    break;
                case JET_coltyp.Currency:
                    data = new byte[8];
                    rand.NextBytes(data);
                    break;
                case JET_coltyp.IEEESingle:
                    data = new byte[4];
                    rand.NextBytes(data);
                    break;
                case JET_coltyp.IEEEDouble:
                    data = new byte[8];
                    rand.NextBytes(data);
                    break;
                case JET_coltyp.DateTime:
                    data = new byte[8];
                    rand.NextBytes(data);
                    break;
                case JET_coltyp.Binary:
                {
                    int size = rand.Next(255) + 1;
                    data = GetRandomBinaryBytes(size, rand);
                    break;
                }

                case JET_coltyp.Text:
                {
                    // GetRandomUnicodeBytes will round up the size of 
                    // an odd-sized request so a request for 255 bytes will
                    // give a 256 byte buffer, which is too large. Restrict
                    // unicode columns to 254 bytes.
                    int size = (JET_CP.ASCII == cp) ? rand.Next(255) : rand.Next(254) + 1;
                    data = (JET_CP.ASCII == cp) ? GetRandomAsciiBytes(size, rand) : GetRandomUnicodeBytes(size, rand);
                    break;
                }

                case JET_coltyp.LongBinary:
                {
                    int size = rand.Next(9 * 1024) + 1;
                    data = GetRandomBinaryBytes(size, rand);
                    break;
                }

                case JET_coltyp.LongText:
                {
                    int size = rand.Next(9 * 1024) + 1;
                    data = (JET_CP.ASCII == cp) ? GetRandomAsciiBytes(size, rand) : GetRandomUnicodeBytes(size, rand);
                    break;
                }

                case VistaColtyp.UnsignedLong:
                    data = new byte[4];
                    rand.NextBytes(data);
                    break;
                case VistaColtyp.LongLong:
                    data = new byte[8];
                    rand.NextBytes(data);
                    break;
                case VistaColtyp.GUID:
                    data = new byte[16];
                    rand.NextBytes(data);
                    break;
                case VistaColtyp.UnsignedShort:
                    data = new byte[2];
                    rand.NextBytes(data);
                    break;
                default:
                    throw new Exception("Invalid coltyp");
            }

            return data;
        }

        /// <summary>
        /// Returns a string of random, printable (ASCII) characters
        /// </summary>
        /// <param name="length">The length of the string to generate.</param>
        /// <param name="r">The random number generator.</param>
        /// <returns>A random string.</returns>
        private static string MakeRandomString(int length, Random r)
        {
            var sb = new StringBuilder(length);
            for (int i = 0; i < length; ++i)
            {
                // printable ASCII characters are 32 (space) through 126 (~)
                int c = r.Next(32, 127);
                sb.Append((char)c);
            }

            return sb.ToString();
        }

        /// <summary>
        /// Returns an array of up to maxLength bytes, filled with random data
        /// </summary>
        /// <param name="length">The length of the array to generate.</param>
        /// <param name="r">The random number generator.</param>
        /// <returns>A random string.</returns>
        private static byte[] GetRandomBinaryBytes(int length, Random r)
        {
            var bytes = new byte[length];
            r.NextBytes(bytes);
            return bytes;
        }

        /// <summary>
        /// Returns an array of up to maxLength bytes, filled with random ASCII characters
        /// </summary>
        /// <param name="length">The length of the array to generate.</param>
        /// <param name="r">The random number generator.</param>
        /// <returns>A random array.</returns>
        private static byte[] GetRandomAsciiBytes(int length, Random r)
        {
            string s = MakeRandomString(length, r);
            return Encoding.ASCII.GetBytes(s);
        }

        /// <summary>
        /// Returns an array of up to maxLength bytes, filled with random Unicode characters
        /// </summary>
        /// <param name="length">The length of the array to generate.</param>
        /// <param name="r">The random number generator.</param>
        /// <returns>A random array.</returns>
        private static byte[] GetRandomUnicodeBytes(int length, Random r)
        {
            string s = MakeRandomString((length + 1) / 2, r); // Unicode characters are 2 bytes wide
            return Encoding.Unicode.GetBytes(s);
        }
    }
}