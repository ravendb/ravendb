using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers
{
    public class NGramTokenFilter : TokenFilter
    {
        public static int DEFAULT_MIN_NGRAM_SIZE = 1;
        public static int DEFAULT_MAX_NGRAM_SIZE = 2;

        private readonly int _minGram;
        private readonly int _maxGram;

        private char[] _curTermBuffer;
        private int _curTermLength;
        private int _curGramSize;
        private int _curPos;
        private int _tokStart;

        private readonly TermAttribute _termAtt;
        private readonly OffsetAttribute _offsetAtt;

        /**
         * Creates NGramTokenFilter with given min and max n-grams.
         * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
         * <param name="minGram">the smallest n-gram to generate</param>
         * <param name="maxGram">the largest n-gram to generate</param>
         */
        public NGramTokenFilter(TokenStream input, int minGram, int maxGram)
            : base(input)
        {

            if (minGram < 1)
            {
                throw new System.ArgumentException("minGram must be greater than zero");
            }
            if (minGram > maxGram)
            {
                throw new System.ArgumentException("minGram must not be greater than maxGram");
            }
            _minGram = minGram;
            _maxGram = maxGram;

            _termAtt = (TermAttribute)AddAttribute<ITermAttribute>();
            _offsetAtt = (OffsetAttribute)AddAttribute<IOffsetAttribute>();
        }

        /**
         * Creates NGramTokenFilter with default min and max n-grams.
         * <param name="input"><see cref="TokenStream"/> holding the input to be tokenized</param>
         */
        public NGramTokenFilter(TokenStream input)
            : this(input, DEFAULT_MIN_NGRAM_SIZE, DEFAULT_MAX_NGRAM_SIZE)
        {

        }

        /** Returns the next token in the stream, or null at EOS. */
        public override bool IncrementToken()
        {
            while (true)
            {
                if (_curTermBuffer == null)
                {
                    if (!input.IncrementToken())
                    {
                        return false;
                    }
                    else
                    {
                        _curTermBuffer = (char[])_termAtt.TermBuffer().Clone();
                        _curTermLength = _termAtt.TermLength();
                        _curGramSize = _minGram;
                        _curPos = 0;
                        _tokStart = _offsetAtt.StartOffset;
                    }
                }
                while (_curGramSize <= _maxGram)
                {
                    if (_curPos + _curGramSize <= _curTermLength)
                    {     // while there is input
                        ClearAttributes();
                        _termAtt.SetTermBuffer(_curTermBuffer, _curPos, _curGramSize);
                        _offsetAtt.SetOffset(_tokStart + _curPos, _tokStart + _curPos + _curGramSize);
                        _curPos++;
                        return true;
                    }

                    _curGramSize++;                         // increase n-gram size
                    _curPos = 0;
                }
                _curTermBuffer = null;
            }
        }

        public override void Reset()
        {
            base.Reset();
            _curTermBuffer = null;
        }
    }
}
