using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers
{
    public struct LowerCaseWhitespaceTokenizerHelper : ILowerCaseTokenizerHelper
    {
        private static readonly TextInfo _invariantTextInfo = CultureInfo.InvariantCulture.TextInfo;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsTokenChar(char c)
        {
            return char.IsWhiteSpace(c) == false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public char Normalize(char c)
        {
            int cInt = c;
            if (cInt < 128)
            {
                if (65 <= cInt && cInt <= 90)
                    c |= ' ';

                return c;
            }

            return _invariantTextInfo.ToLower(c);
        }
    }

    public class LowerCaseWhitespaceTokenizer : LowerCaseTokenizerBase<LowerCaseWhitespaceTokenizerHelper>
    {
        public LowerCaseWhitespaceTokenizer(TextReader input) : base(input)
        {
        }

    }
}
