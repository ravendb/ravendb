//-----------------------------------------------------------------------
// <copyright file="CollationKeyFilter.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Globalization;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

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
            var key = _cultureInfo.CompareInfo.GetSortKey(text);
            return key.KeyData;
        }
    }
}
