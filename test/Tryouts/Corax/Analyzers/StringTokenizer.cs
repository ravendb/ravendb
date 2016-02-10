using System.IO;
using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public class StringTokenizer : ITokenSource
    {
        private bool _quoted;
        private LazyStringValue _term;
        
        public void SetReader(LazyStringValue reader)
        {
            _term = reader;
            Start = 0;
            Position = 0;
            Size = 0;
            _quoted = false;
        }

        public unsafe LazyStringValue GetCurrent()
        {
            return new LazyStringValue(null, _term.Buffer+Start, Size, _term.Context);
        }

        public int Start { get; set; }

        public int Size { get; set; }

        public int Line { get; private set; }

        public int Column { get; private set; }
        public int Position { get; set; }

        public unsafe bool Next()
        {
            Start += Size;
            Size = 0;
            byte ch = 0;
            while (Position < _term.Size)
            {
                byte prev = ch;
                ch = _term.Buffer[Position++];
                Column++;
                if (Position == _term.Size) // EOF
                {
                    if (_quoted && Size > 0)
                    {
                        // we have an unterminated string, so we will ignore the quote, instead of errorring
                        Start++; // skip quote
                        return true;
                    }
                    return Size > 0;
                }
                if (ch == '\r' || ch == '\n')
                {
                    Column = 0;
                    if (prev != '\r' || ch != '\n')
                    {
                        Line++; // only move to new line if it isn't the \n in a \r\n pair
                    }
                    if (_quoted)
                    {
                        Size++;
                    }
                    else if (Size > 0)
                        return true;
                    continue;
                }
                if (char.IsWhiteSpace((char)ch))
                {
                    if (_quoted) // for a quoted string, we will continue until the end of the string
                    {
                        Size++;
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

                if (char.IsPunctuation((char)ch))
                {
                    // if followed by whitespace, ignore
                    if(Position+1 < _term.Size && char.IsWhiteSpace((char)_term.Buffer[Position + 1]))
                        continue;
                }

                Size++;
            }
            return Size > 0;
        }

        public override string ToString()
        {
            return _term.ToString();
        }
    }
}