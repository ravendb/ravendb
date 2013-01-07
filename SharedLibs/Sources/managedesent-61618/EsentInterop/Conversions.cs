//-----------------------------------------------------------------------
// <copyright file="Conversions.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    /// <summary>
    /// Provide methods to convert data and flags between 
    /// Win32 and the .NET Framework.
    /// </summary>
    public static class Conversions
    {
        /// <summary>
        /// Maps a CompareOption enumeration to the corresponding LCMapString flag.
        /// </summary>
        private static readonly IDictionary<CompareOptions, uint> compareOptionsToLcmapFlags;

        /// <summary>
        /// Maps an LCMapString flag to the corresponding CompareOption enumeration.
        /// </summary>
        private static readonly IDictionary<uint, CompareOptions> lcmapFlagsToCompareOptions;

        /// <summary>
        /// Initializes static members of the Conversions class. This sets up the
        /// conversion mapping dictionaries.
        /// </summary>
        static Conversions()
        {
            // Rather than creating both dictionaries, define one as the inverse of the other.
            compareOptionsToLcmapFlags = new Dictionary<CompareOptions, uint>
            {
                { CompareOptions.IgnoreCase, NativeMethods.NORM_IGNORECASE },
                { CompareOptions.IgnoreKanaType, NativeMethods.NORM_IGNOREKANATYPE },
                { CompareOptions.IgnoreNonSpace, NativeMethods.NORM_IGNORENONSPACE },
                { CompareOptions.IgnoreSymbols, NativeMethods.NORM_IGNORESYMBOLS },
                { CompareOptions.IgnoreWidth, NativeMethods.NORM_IGNOREWIDTH },
                { CompareOptions.StringSort, NativeMethods.SORT_STRINGSORT }
            };

            lcmapFlagsToCompareOptions = InvertDictionary(compareOptionsToLcmapFlags);
        }

        /// <summary>
        /// Convert a double (OA date time format) to a DateTime. Unlike DateTime.FromOADate
        /// this doesn't throw exceptions.
        /// </summary>
        /// <param name="d">The double value.</param>
        /// <returns>A DateTime.</returns>
        public static DateTime ConvertDoubleToDateTime(double d)
        {
            try
            {
                return DateTime.FromOADate(d);
            }
            catch (ArgumentException)
            {
                // Not all double values are valid OADates. We deal with out-of-range values
                // by returning either min or max
                return d < 0 ? DateTime.MinValue : DateTime.MaxValue;
            }
        }

        /// <summary>
        /// Given flags for LCMapFlags, turn them into compare options. Unknown options 
        /// are ignored.
        /// </summary>
        /// <param name="lcmapFlags">LCMapString flags.</param>
        /// <returns>CompareOptions describing the (known) flags.</returns>
        [CLSCompliant(false)]
        public static CompareOptions CompareOptionsFromLCMapFlags(uint lcmapFlags)
        {
            // This should be a template, but there isn't an elegant way to express than with C# generics
            CompareOptions options = CompareOptions.None;
            foreach (uint flag in lcmapFlagsToCompareOptions.Keys)
            {
                if (flag == (lcmapFlags & flag))
                {
                    options |= lcmapFlagsToCompareOptions[flag];
                }
            }

            return options;
        }

        /// <summary>
        /// Give CompareOptions, turn them into flags from LCMapString. Unknown options are ignored.
        /// </summary>
        /// <param name="compareOptions">The options to convert.</param>
        /// <returns>The LCMapString flags that match the compare options. Unsupported options are ignored.</returns>
        [CLSCompliant(false)]
        public static uint LCMapFlagsFromCompareOptions(CompareOptions compareOptions)
        {
            // This should be a template, but there isn't an elegant way to express than with C# generics
            uint flags = 0;
            foreach (CompareOptions option in compareOptionsToLcmapFlags.Keys)
            {
                if (option == (compareOptions & option))
                {
                    flags |= compareOptionsToLcmapFlags[option];
                }
            }

            return flags;
        }

        /// <summary>
        /// Given a Key=>Value dictionary create an inverted dictionary that maps Value=>Key.
        /// </summary>
        /// <typeparam name="TValue">The new value type (the key of the current dictionary).</typeparam>
        /// <typeparam name="TKey">The new key type (the value if the current dictionary).</typeparam>
        /// <param name="dict">The dictionary to invert.</param>
        /// <returns>An inverted dictionary.</returns>
        private static IDictionary<TKey, TValue> InvertDictionary<TValue, TKey>(ICollection<KeyValuePair<TValue, TKey>> dict)
        {
            var invertedDict = new Dictionary<TKey, TValue>(dict.Count);
            foreach (KeyValuePair<TValue, TKey> entry in dict)
            {
                invertedDict.Add(entry.Value, entry.Key);
            }

            return invertedDict;
        }

        /// <summary>
        /// This class contains the unmanaged constants used in the conversion.
        /// </summary>
        internal static class NativeMethods
        {
            #region Win32 Constants

            /// <summary>
            /// Ignore case.
            /// </summary>
            public const uint NORM_IGNORECASE = 0x00000001;

            /// <summary>
            /// Ignore nonspacing chars.
            /// </summary>
            public const uint NORM_IGNORENONSPACE = 0x00000002;

            /// <summary>
            /// Ignore symbols.
            /// </summary>
            public const uint NORM_IGNORESYMBOLS = 0x00000004;

            /// <summary>
            /// Inore kanatype.
            /// </summary>
            public const uint NORM_IGNOREKANATYPE = 0x00010000;

            /// <summary>
            /// Ignore width.
            /// </summary>
            public const uint NORM_IGNOREWIDTH = 0x00020000;

            /// <summary>
            /// Treat punctuation the same as symbols.
            /// </summary>
            public const uint SORT_STRINGSORT = 0x00001000;

            /// <summary>
            /// Produce a normalized wide-character sort key.
            /// </summary>
            public const uint LCMAP_SORTKEY = 0x00000400;

            #endregion Win32 Constants
        }
    }
}
