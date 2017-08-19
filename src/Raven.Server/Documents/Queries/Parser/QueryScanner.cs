using System;

namespace Raven.Server.Documents.Queries.Parser
{
    public class QueryScanner
    {
        private int _pos;
        private string _q;
        public int Column, Line;
        public int EscapeChars;
        public int TokenStart, TokenLength;
        public string Input => _q;

        public int Position => _pos;
        public string CurrentToken => _q.Substring(TokenStart, TokenLength);

        public override string ToString()
        {
            if (_pos == 0)
                return _q;
            return "..." + _q.Substring(_pos);
        }

        public void Init(string q)
        {
            _q = q ?? string.Empty;
            _pos = 0;
            Column = 1;
            Line = 1;
            TokenStart = 0;
            TokenLength = 0;
        }

        public bool NextToken()
        {
            if (SkipWhitespace() == false)
                return false;
            TokenStart = _pos;

            for (; _pos < _q.Length; _pos++)
                if (char.IsWhiteSpace(_q[_pos]))
                    break;
            TokenLength = _pos - TokenStart;
            return true;
        }

        public NumberToken? TryNumber()
        {
            if (SkipWhitespace() == false)
                return null;

            if (char.IsDigit(_q[_pos]) == false && _q[_pos] != '-')
                return null;

            var result = NumberToken.Long;
            TokenStart = _pos;
            var i = _pos + 1;
            for (; i < _q.Length; i++)
            {
                var c = _q[i];
                if (c >= '0' && c <= '9')
                    continue;

                if (c != '.')
                    break;

                if (result == NumberToken.Double)
                    return null; // we already saw this, so can't allow double periods
                result = NumberToken.Double;
            }

            Column += i - _pos;
            _pos = i;

            TokenLength = _pos - TokenStart;
            return result;
        }

        public bool Identifier(bool skipWhitespace = true, bool beginning = true)
        {
            if (SkipWhitespace(skipWhitespace) == false)
                return false;

            if ((beginning ? char.IsLetter(_q[_pos]) == false : char.IsLetterOrDigit(_q[_pos]) == false) && _q[_pos] != '_' && _q[_pos] != '@')
                return false;

            TokenStart = _pos;
            _pos++;

            for (; _pos < _q.Length; _pos++)
                if (char.IsLetterOrDigit(_q[_pos]) == false && _q[_pos] != '_' && _q[_pos] != '-')
                    break;
            TokenLength = _pos - TokenStart;
            Column += TokenLength;
            return true;
        }

        private bool SkipWhitespace(bool skipWhitespace = true)
        {
            if (skipWhitespace == false)
                return _pos < _q.Length;

            for (; _pos < _q.Length; _pos++, Column++)
                switch (_q[_pos])
                {
                    case ' ':
                    case '\t':
                    case '\r':
                        continue;
                    case '\n':
                        Line++;
                        Column = 1;
                        break;
                    case '/': // comment to end of line / input
                    {
                        if (_pos + 1 >= _q.Length || _q[_pos + 1] != '/')
                            return true;
                        _pos += 2;
                        for (; _pos < _q.Length; _pos++)
                            if (_q[_pos] == '\n')
                                goto case '\n';
                        return false; // end of input
                    }
                    default:
                        return true;
                }
            return false;
        }

        public bool TryScan(char match, bool skipWhitespace = true)
        {
            if (SkipWhitespace(skipWhitespace) == false)
                return false;

            if (_q[_pos] != match)
                return false;
            _pos++;
            Column++;
            return true;
        }
        
        public bool TryPeek(char match, bool skipWhitespace = true)
        {
            if (SkipWhitespace(skipWhitespace) == false)
                return false;

            return _q[_pos] == match;
        }

        public bool TryScan(string match, bool skipWhitespace = true)
        {
            if (SkipWhitespace(skipWhitespace) == false)
                return false;

            if (match.Length + _pos > _q.Length)
                return false;

            if (string.Compare(_q, _pos, match, 0, match.Length, StringComparison.OrdinalIgnoreCase) != 0)
                return false;

            if (_pos + match.Length < _q.Length)
            {
                if (char.IsLetterOrDigit(match[match.Length - 1]) &&
                   char.IsLetterOrDigit(_q[_pos + match.Length]))
                    return false;
            }

            _pos += match.Length;
            Column += match.Length;
            return true;
        }

        public bool TryScan(string[] matches, out string found)
        {
            if (SkipWhitespace() == false)
            {
                found = null;
                return false;
            }

            foreach (var match in matches)
            {
                if (match.Length + _pos > _q.Length)
                    continue;

                if (string.Compare(_q, _pos, match, 0, match.Length, StringComparison.OrdinalIgnoreCase) != 0)
                    continue;

                if (_pos + match.Length < _q.Length)
                {
                    if (char.IsLetterOrDigit(match[match.Length - 1]) &&
                       char.IsLetterOrDigit(_q[_pos + match.Length]))
                        continue;
                }

                _pos += match.Length;
                Column += match.Length;
                found = match;
                return true;
            }

            found = null;
            return false;
        }

        public bool String()
        {
            EscapeChars = 0;
            if (SkipWhitespace() == false)
                return false;

            var quoteChar = _q[_pos];

            if (quoteChar != '"' && quoteChar != '\'')
                return false;
            TokenStart = _pos;
            var i = _pos + 1;
            for (; i < _q.Length; i++)
            {
                if (_q[i] != quoteChar)
                    continue;

                if (i + 1 < _q.Length && _q[i + 1] == quoteChar)
                {
                    i++; // escape char
                    EscapeChars++;
                    continue;
                }
                Column += i + 1 - _pos;

                _pos = i + 1;
                TokenLength = _pos - TokenStart;
                return true;
            }

            return false;
        }

        public void Reset(int pos)
        {
            _pos= pos;
        }

        public bool FunctionBody()
        {
            var original = _pos;
            if (TryScan('{') == false)
                return false;

            // now we need to find the matching }, this is 
            // a bit more complex because we need to ignore
            // matching } that are in quotes. 

            int nested = 1;
            for (; _pos < _q.Length; _pos++)
            {
                switch (_q[_pos])
                {
                    case '"':
                    case '\'':
                        if (String() == false)
                            goto Failed;
                        break;
                    case '{':
                        nested++;
                        break;
                    case '}':
                        if (--nested == 0)
                        {
                            _pos += 1;
                            TokenStart = original;
                            TokenLength = _pos - original;
                            return true;
                        }
                        break;
                }
            }
            Failed:
            _pos = original;
            return false;

        }
    }
}
