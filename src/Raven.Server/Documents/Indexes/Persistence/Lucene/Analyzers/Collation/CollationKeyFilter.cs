//-----------------------------------------------------------------------
// <copyright file="CollationKeyFilter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Security;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Sparrow.Platform;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.Collation
{
    public class CollationKeyFilter : TokenFilter
    {
        private readonly TermAttribute _termAtt;
        private readonly CultureInfo _cultureInfo;

        public CollationKeyFilter(TokenStream input, CultureInfo cultureInfo) : base(input)
        {
            _cultureInfo = cultureInfo;
            _termAtt = (TermAttribute)AddAttribute<ITermAttribute>();
        }

        public override bool IncrementToken()
        {
            if (input.IncrementToken() == false)
                return false;

            var termBuffer = _termAtt.TermBuffer();
            var termText = new string(termBuffer, 0, _termAtt.TermLength());
            var collationKey = GetCollationKey(termText);
            var encodedLength = IndexableBinaryStringTools_UsingArrays.GetEncodedLength(collationKey);
            if (encodedLength > termBuffer.Length)
                termBuffer = _termAtt.ResizeTermBuffer(encodedLength);

            _termAtt.SetTermLength(encodedLength);
            IndexableBinaryStringTools_UsingArrays.Encode(collationKey, termBuffer);

            return true;
        }

        private byte[] GetCollationKey(string text)
        {
            if (PlatformDetails.RunningOnPosix)
                return GetCollationKeyPosix(text);

            return GetCollationKeyWin32(text);
        }

        private unsafe byte[] GetCollationKeyPosix(string text)
        {
            var sortHandle = PosixHelper.Instance.GetSortHandle(_cultureInfo.CompareInfo);

            var length = PosixNativeMethods.GetSortKey(sortHandle, text, text.Length, null, 0, CompareOptions.None);
            if (length == 0)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to GetSortKey for text=" + text);

            var sortKey = new byte[length];

            fixed (byte* pSortKey = sortKey)
            {
                length = PosixNativeMethods.GetSortKey(sortHandle, text, text.Length, pSortKey, sortKey.Length, CompareOptions.None);
                if (length == 0)
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to GetSortKey for text=" + text);
                return sortKey;
            }
        }

        private unsafe byte[] GetCollationKeyWin32(string text)
        {
            var length = Win32NativeMethods.LCMapStringEx(_cultureInfo.CompareInfo.Name, Win32NativeMethods.LCMAP_SORTKEY, text, text.Length, IntPtr.Zero, 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            if (length == 0)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to GetSortKey for text=" + text);

            var sortKey = new byte[length];

            fixed (byte* pSortKey = sortKey)
            {
                length = Win32NativeMethods.LCMapStringEx(_cultureInfo.CompareInfo.Name, Win32NativeMethods.LCMAP_SORTKEY, text, text.Length, (IntPtr)pSortKey, sortKey.Length, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
                if (length == 0)
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "Failed to GetSortKey for text=" + text);

                return sortKey;
            }
        }

        private static class Win32NativeMethods
        {
            public const uint LCMAP_SORTKEY = 0x00000400;

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            public static extern int LCMapStringEx(
                string lpLocaleName,
                uint dwMapFlags,
                string lpSrcStr,
                int cchSrc,
                [Out] IntPtr lpDestStr,
                int cchDest,
                IntPtr lpVersionInformation,
                IntPtr lpReserved,
                IntPtr sortHandle);
        }

        private static class PosixNativeMethods
        {
            [SecurityCritical]
            [DllImport("System.Globalization.Native", CharSet = CharSet.Unicode, EntryPoint = "GlobalizationNative_GetSortKey")]
            public static extern unsafe int GetSortKey(SafeHandle sortHandle, string str, int strLength, byte* sortKey, int sortKeyLength, CompareOptions options);
        }

        private sealed class PosixHelper
        {
            public delegate SafeHandle GetSortHandleDelegate(CompareInfo value);

            public static readonly PosixHelper Instance = new PosixHelper();

            public readonly GetSortHandleDelegate GetSortHandle;

            private PosixHelper()
            {
                GetSortHandle = CreateGetSortHandleMethod().Compile();
            }

            private static Expression<GetSortHandleDelegate> CreateGetSortHandleMethod()
            {
                var parameter = Expression.Parameter(typeof(CompareInfo), "value");
                var member = Expression.Field(parameter, "_sortHandle");

                return Expression.Lambda<GetSortHandleDelegate>(member, parameter);
            }
        }
    }
}
