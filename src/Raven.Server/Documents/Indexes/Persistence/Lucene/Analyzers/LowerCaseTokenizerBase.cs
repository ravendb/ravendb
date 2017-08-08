using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers
{
    public interface ILowerCaseTokenizerHelper
    {
        /// <summary>Returns true iff a character should be included in a token.  This
        /// tokenizer generates as tokens adjacent sequences of characters which
        /// satisfy this predicate.  Characters for which this is false are used to
        /// define token boundaries and are not included in tokens. 
        /// </summary>
        bool IsTokenChar(char c);

        /// <summary>Called on each token character to normalize it before it is added to the
        /// token.  The default implementation does nothing. Subclasses may use this
        /// to, e.g., lowercase tokens. 
        /// </summary>
        char Normalize(char c);
    }

    public class LowerCaseTokenizerBase<T> : Tokenizer where T : struct, ILowerCaseTokenizerHelper
    {
        // PERF: This helper will act as a inheritance dispatcher that can be modified but at the same time
        //       ensure these performance critical calls will get inlined and optimized accordingly.
        private static readonly T Helper = default(T);

        public LowerCaseTokenizerBase(System.IO.TextReader input)
            : base(input)
        {
            _offsetAtt = AddAttribute<IOffsetAttribute>();
            _termAtt = AddAttribute<ITermAttribute>();
        }

        protected LowerCaseTokenizerBase(AttributeSource source, System.IO.TextReader input)
            : base(source, input)
        {
            _offsetAtt = AddAttribute<IOffsetAttribute>();
            _termAtt = AddAttribute<ITermAttribute>();
        }

        protected LowerCaseTokenizerBase(AttributeFactory factory, System.IO.TextReader input)
            : base(factory, input)
        {
            _offsetAtt = AddAttribute<IOffsetAttribute>();
            _termAtt = AddAttribute<ITermAttribute>();
        }

        private int _offset, _bufferIndex, _dataLen;

        private const int IO_BUFFER_SIZE = 4096;
        private readonly char[] _ioBuffer = new char[IO_BUFFER_SIZE];
        private readonly ITermAttribute _termAtt;
        private readonly IOffsetAttribute _offsetAtt;

        public override bool IncrementToken()
        {
            ClearAttributes();

            int length = 0;
            int start = _bufferIndex;

            char[] buffer = _termAtt.TermBuffer();
            while (true)
            {
                if (_bufferIndex >= _dataLen)
                {
                    _offset += _dataLen;
                    _dataLen = input.Read(_ioBuffer, 0, _ioBuffer.Length);
                    if (_dataLen <= 0)
                    {
                        _dataLen = 0; // so next offset += dataLen won't decrement offset
                        if (length > 0)
                            break;

                        return false;
                    }
                    _bufferIndex = 0;
                }

                char c = _ioBuffer[_bufferIndex++];

                if (Helper.IsTokenChar(c))
                {
                    // if it's a token char

                    if (length == 0)
                        // start of token
                        start = _offset + _bufferIndex - 1;
                    else if (length == buffer.Length)
                        buffer = _termAtt.ResizeTermBuffer(1 + length);

                    buffer[length++] = Helper.Normalize(c); // buffer it, normalized
                }
                else if (length > 0)
                    // at non-Letter w/ chars
                    break; // return 'em
            }

            _termAtt.SetTermLength(length);
            _offsetAtt.SetOffset(CorrectOffset(start), CorrectOffset(start + length));

            return true;
        }

        public override void End()
        {
            // set final offset
            int finalOffset = CorrectOffset(_offset);
            _offsetAtt.SetOffset(finalOffset, finalOffset);
        }

        public override void Reset(System.IO.TextReader tr)
        {
            base.Reset(tr);
            _bufferIndex = 0;
            _offset = 0;
            _dataLen = 0;
        }
    }
}
