//-----------------------------------------------------------------------
// <copyright file="StringCache.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Class that helps cache strings.
    /// </summary>
    internal static class StringCache
    {
        /// <summary>
        /// Don't cache strings whose length is longer than this.
        /// </summary>
        private const int MaxLengthToCache = 128;

        /// <summary>
        /// Number of converted strings to hash.
        /// </summary>
        private const int NumCachedBoxedValues = 1031;

        /// <summary>
        /// Cached string values.
        /// </summary>
        private static readonly string[] cachedStrings = new string[NumCachedBoxedValues];

        /// <summary>
        /// Return the interned version of a string, or the original
        /// string if it isn't interned.
        /// </summary>
        /// <param name="s">The string to try to intern.</param>
        /// <returns>An interned copy of the string or the original string.</returns>
        public static string TryToIntern(string s)
        {
            return String.IsInterned(s) ?? s;
        }

        /// <summary>
        /// Convert a byte array to a string.
        /// </summary>
        /// <param name="value">The bytes to convert.</param>
        /// <param name="startIndex">The starting index of the data to convert.</param>
        /// <param name="count">The number of bytes to convert.</param>
        /// <returns>A string converted from the data.</returns>
        public static string GetString(byte[] value, int startIndex, int count)
        {
            unsafe
            {
                fixed (byte* data = value)
                {
                    char* chars = (char*)(data + startIndex);
                    return GetString(chars, 0, count / sizeof(char));
                }
            }            
        }

        /// <summary>
        /// Convert a char array to a string, using a cached value if possible.
        /// </summary>
        /// <param name="value">The characters to convert.</param>
        /// <param name="startIndex">The starting index of the data to convert.</param>
        /// <param name="count">The number of characters to convert.</param>
        /// <returns>A string converted from the data.</returns>
        private static unsafe string GetString(char* value, int startIndex, int count)
        {
            string s;

            if (0 == count)
            {
                s = String.Empty;
            }
            else if (count < MaxLengthToCache)
            {
                uint hash = CalculateHash(value, startIndex, count);
                int index = unchecked((int)(hash % NumCachedBoxedValues));
                s = cachedStrings[index];
                if (null == s || !AreEqual(s, value, startIndex, count))
                {
                    s = CreateNewString(value, startIndex, count);
                    cachedStrings[index] = s;
                }
            }
            else
            {
                s = CreateNewString(value, startIndex, count);
            }

            return s;
        }

        /// <summary>
        /// Calculate the hash of a string.
        /// </summary>
        /// <param name="value">The characters to hash.</param>
        /// <param name="startIndex">The starting index of the data to hash.</param>
        /// <param name="count">The number of characters to hash.</param>
        /// <returns>The hash value of the data.</returns>
        private static unsafe uint CalculateHash(char* value, int startIndex, int count)
        {
            uint hash = 0;
            unchecked
            {
                for (int i = 0; i < count; ++i)
                {
                    hash ^= value[startIndex + i];
                    hash *= 33;
                }                
            }

            return hash;
        }

        /// <summary>
        /// Determine if a string matches a char array..
        /// </summary>
        /// <param name="s">The string to compare against.</param>
        /// <param name="value">The characters.</param>
        /// <param name="startIndex">The starting index of the data.</param>
        /// <param name="count">The number of characters.</param>
        /// <returns>True if the string matches the char array.</returns>
        private static unsafe bool AreEqual(string s, char* value, int startIndex, int count)
        {
            if (s.Length != count)
            {
                return false;
            }

            unchecked
            {
                for (int i = 0; i < s.Length; ++i)
                {
                    if (s[i] != value[startIndex + i])
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Convert a char array to a string.
        /// </summary>
        /// <param name="value">The characters to convert.</param>
        /// <param name="startIndex">The starting index of the data to convert.</param>
        /// <param name="count">The number of characters to convert.</param>
        /// <returns>A string converted from the data.</returns>
        private static unsafe string CreateNewString(char* value, int startIndex, int count)
        {
            // Encoding.Unicode.GetString copies the data to an array of chars and then
            // makes a string from it, copying the data twice. Use the more efficient
            // char* constructor.
            return new string(value, startIndex, count);
        }
    }
}