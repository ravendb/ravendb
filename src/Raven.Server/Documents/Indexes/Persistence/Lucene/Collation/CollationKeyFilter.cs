//-----------------------------------------------------------------------
// <copyright file="CollationKeyFilter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Sparrow;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Collation
{
    public class CollationKeyFilter : TokenFilter
    {
        private readonly TermAttribute _termAtt;
        private readonly CultureInfo _cultureInfo;

        public CollationKeyFilter(TokenStream input, CultureInfo cultureInfo) : base(input)
        {
            _cultureInfo = cultureInfo;
            _termAtt = (TermAttribute)base.AddAttribute<ITermAttribute>();
        }

        public override bool IncrementToken()
        {
            if (input.IncrementToken() == false)
                return false;

            var termBuffer = _termAtt.TermBuffer();
            var termText = new string(termBuffer, 0, _termAtt.TermLength());
            var collationKey = GetCollationKey(termText);
            var encodedLength = IndexableBinaryStringTools_UsingArrays.GetEncodedLength(collationKey);
            if (sizeof(int) > termBuffer.Length)
                termBuffer = _termAtt.ResizeTermBuffer(encodedLength);

            _termAtt.SetTermLength(encodedLength);
            IndexableBinaryStringTools_UsingArrays.Encode(collationKey, termBuffer);

            return true;
        }

        private unsafe byte[] GetCollationKey(string text)
        {
            if (Platform.RunningOnPosix)
                throw new NotImplementedException();

            var length = Win32NativeMethods.LCMapStringEx(_cultureInfo.CompareInfo.Name, Win32NativeMethods.LCMAP_SORTKEY, text, text.Length, IntPtr.Zero, 0, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            if (length == 0)
                throw new Win32Exception();

            var result = new byte[length];

            fixed (byte* r = result)
            {
                length = Win32NativeMethods.LCMapStringEx(_cultureInfo.CompareInfo.Name, Win32NativeMethods.LCMAP_SORTKEY, text, text.Length, (IntPtr)r, result.Length, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
                if (length == 0)
                    throw new Win32Exception();

                return result;
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
    }
}
