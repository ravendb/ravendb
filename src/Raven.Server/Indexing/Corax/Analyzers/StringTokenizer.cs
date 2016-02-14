using System.IO;
using System.Runtime.CompilerServices;

namespace Raven.Server.Indexing.Corax.Analyzers
{
    public class StringTokenizer : ITokenSource
    {
        private bool _quoted;
        private TextReader _reader;

        public StringTokenizer(int maxBufferSize = 256)
        {
            Buffer = new char[maxBufferSize];
        }

        public void SetReader(TextReader reader)
        {
            _reader = reader;
            Size = 0;
            _quoted = false;
        }

        public char[] Buffer { get; }

        public int Size { get; set; }

        public int Line { get; private set; }

        public int Column { get; private set; }
        public int Position { get; set; }

        private bool BufferFull
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Buffer.Length == Size; }
        }

        public bool Next()
        {
            Size = 0;
            Position++;
            char ch = '\0';
            while (true)
            {
                char prev = ch;
                int r = _reader.Read();
                Column++;
                if (r == -1) // EOF
                {
                    if (_quoted && Size > 0)
                    {
                        // we have an unterminated string, so we will ignore the quote, instead of errorring
                        SetReader(new StringReader(new string(Buffer, 0, Size)));
                        ch = '\0';
                        continue;
                    }
                    return Size > 0;
                }

                ch = (char)r;
                if (ch == '\r' || ch == '\n')
                {
                    Column = 0;
                    if (prev != '\r' || ch != '\n')
                    {
                        Line++; // only move to new line if it isn't the \n in a \r\n pair
                    }
                    if (_quoted)
                    {
                        AppendToBuffer(ch);
                        if (BufferFull)
                        {
                            return true;
                        }
                    }
                    else if (Size > 0)
                        return true;
                    continue;
                }
                if (char.IsWhiteSpace(ch))
                {
                    if (_quoted) // for a quoted string, we will continue until the end of the string
                    {
                        AppendToBuffer(ch);
                        if (BufferFull)
                        {
                            return true;
                        }
                    }
                    else if (Size > 0) // if we have content before, we will return this token
                        return true;
                    continue;
                }
                if (ch == '"')
                {
                    if (_quoted == false)
                    {
                        _quoted = true;
                        if (Size > 0)
                            return true; // return the current token
                        continue;
                    }
                    _quoted = false;
                    return true;
                }

                if (char.IsPunctuation(ch))
                {
                    // if followed by whitespace, ignore
                    int next = _reader.Peek();
                    if (next == -1 || char.IsWhiteSpace((char)next))
                        continue;
                }

                AppendToBuffer(ch);
                if (BufferFull)
                    return true;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AppendToBuffer(char ch)
        {
            Buffer[Size++] = ch;
        }

        public override string ToString()
        {
            return new string(Buffer, 0, Size);
        }
    }
}