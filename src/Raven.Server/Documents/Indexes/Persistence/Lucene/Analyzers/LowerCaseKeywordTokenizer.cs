using System.Globalization;
using System.Runtime.CompilerServices;
using Lucene.Net.Util;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers
{
    public struct LowerCaseKeywordTokenizerHelper : ILowerCaseTokenizerHelper
    {
        private static readonly TextInfo _invariantTextInfo = CultureInfo.InvariantCulture.TextInfo;

        /// <summary>Returns true iff a character should be included in a token.  This
        /// tokenizer generates as tokens adjacent sequences of characters which
        /// satisfy this predicate.  Characters for which this is false are used to
        /// define token boundaries and are not included in tokens. 
        /// </summary>
        public bool IsTokenChar(char c)
        {
            return true;
        }

        /// <summary>Called on each token character to normalize it before it is added to the
        /// token.  The default implementation does nothing. Subclasses may use this
        /// to, e.g., lowercase tokens. 
        /// </summary>
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

    public class LowerCaseKeywordTokenizer : LowerCaseTokenizerBase<LowerCaseKeywordTokenizerHelper>
    {
        public LowerCaseKeywordTokenizer(System.IO.TextReader input)
            : base(input)
        { }

        protected LowerCaseKeywordTokenizer(AttributeSource source, System.IO.TextReader input)
            : base(source, input)
        { }

        protected LowerCaseKeywordTokenizer(AttributeFactory factory, System.IO.TextReader input)
            : base(factory, input)
        { }
    }
}
