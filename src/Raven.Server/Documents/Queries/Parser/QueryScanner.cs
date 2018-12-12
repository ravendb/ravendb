using System;
using System.Text;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Documents.Queries.Parser
{
    public class QueryScanner
    {
        private int _pos;
        private string _q;
        public int Column, Line;
        private int _tokenStart, _tokenLength;
        public string Input => _q;

        public StringSegment Token => new StringSegment(_q, _tokenStart, _tokenLength);

        public int Position => _pos;
        public string CurrentToken => _q.Substring(_tokenStart, _tokenLength);

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
            _tokenStart = 0;
            _tokenLength = 0;
        }

        public bool AtEndOfInput()
        {
            if (SkipWhitespace() == false)
                return true;
            return _pos == _q.Length;
        }

        public bool NextPathSegment()
        {
            if (SkipWhitespace() == false)
                return false;
            for (; _pos < _q.Length; _pos++)
                if (char.IsLetter(_q[_pos]))
                    break;
            return true;
        }

        public bool SkipUntil(char match)
        {
            if (SkipWhitespace() == false)
                return false;
            for (; _pos < _q.Length; _pos++)
                if (match == _q[_pos])
                    break;
            return true;
        }

        public bool NextToken()
        {
            if (SkipWhitespace() == false)
                return false;
            _tokenStart = _pos;

            for (; _pos < _q.Length; _pos++)
                if (char.IsWhiteSpace(_q[_pos]))
                    break;
            _tokenLength = _pos - _tokenStart;
            return true;
        }

        public NumberToken? TryNumber()
        {
            if (SkipWhitespace() == false)
                return null;

            if (char.IsDigit(_q[_pos]) == false && _q[_pos] != '-')
                return null;

            var result = NumberToken.Long;
            _tokenStart = _pos;
            var i = _pos + 1;
            for (; i < _q.Length; i++)
            {
                var c = _q[i];
                if (c >= '0' && c <= '9')
                    continue;

                if (c == '_')
                {
                    if (result == NumberToken.Double)
                        return null; // we allow 10_000 only for long numbers, not doubles
                    continue;
                }
                
                if (c != '.')
                    break;

                if (result == NumberToken.Double)
                    return null; // we already saw this, so can't allow double periods
                result = NumberToken.Double;
            }

            Column += i - _pos;
            _pos = i;

            _tokenLength = _pos - _tokenStart;
            return result;
        }

        public bool Identifier(bool skipWhitespace = true, bool beginning = true)
        {
            if (SkipWhitespace(skipWhitespace) == false)
                return false;

            if ((beginning ? char.IsLetter(_q[_pos]) == false : char.IsLetterOrDigit(_q[_pos]) == false) && _q[_pos] != '_' && _q[_pos] != '@')
                return false;

            _tokenStart = _pos;
            _pos++;

            for (; _pos < _q.Length; _pos++)
                if (char.IsLetterOrDigit(_q[_pos]) == false && _q[_pos] != '_' && _q[_pos] != '-')
                    break;
            _tokenLength = _pos - _tokenStart;
            //This covers the cases where the identifier starts with either @@ or _@ but not _
            if(_tokenLength == 1 && (_q[_tokenStart] == '@' || _tokenStart+1<_q.Length && _q[_tokenStart] == '_' && _q[_tokenStart+1] == '@'))
                throw new QueryParser.ParseException(Column + ":" + Line + " Illegal identifier detected starting with "+ _q[_tokenStart] + "@ in query: '" + Input + "'");
            Column += _tokenLength;
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
                    case '/': // comment to end of line / input, /* */ for multi line
                    {
                        if (_pos + 1 >= _q.Length || _q[_pos + 1] != '/' && _q[_pos + 1] != '*')
                            return true;
                        _pos += 2;
                        if (_q[_pos - 1] == '/')
                        {
                            for (; _pos < _q.Length; _pos++)
                                if (_q[_pos] == '\n')
                                    goto case '\n';
                            return false; // end of input
                        }
                        // multi line comment
                        for (; _pos < _q.Length; _pos++)
                        {
                            if (_q[_pos] == '\n')
                                Line++;
                            if (_q[_pos] == '*' && _pos + 1 <= _q.Length && _q[_pos + 1] == '/')
                            {
                                _pos++;
                                break;
                            }
                        }
                        break;// now search for more whitespace / done / eof
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

        public bool TryPeek(string match, bool skipWhitespace = true)
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

            return true;
        }

        public bool TryScan(string match, bool skipWhitespace = true)
        {
            if (TryPeek(match, skipWhitespace) == false)
                return false;

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

        public bool CurrentTokenMatchesAnyOf(string[] options)
        {
            foreach (var match in options)
            {
                if (match.Length != _tokenLength)
                    continue;

                if (string.Compare(_q, _tokenStart, match, 0, match.Length, StringComparison.OrdinalIgnoreCase) != 0)
                    continue;
              
                return true;
            }

            return false;
        }

        public bool String(out StringSegment str)
        {
            if (SkipWhitespace() == false)
            {
                str = default(StringSegment);
                return false;
            }

            var quoteChar = _q[_pos];

            if (quoteChar != '"' && quoteChar != '\'')
            {
                str = default(StringSegment);
                return false;
            }

            _tokenStart = _pos;
            var i = _pos + 1;
            bool hasEscape = false;
            for (; i < _q.Length; i++)
            {
                if (_q[i] == '\\')
                    hasEscape = true;
                else if (_q[i] != quoteChar)
                    continue;

                if (i + 1 < _q.Length && _q[i + 1] == quoteChar)
                {
                    i++; // escape char
                    hasEscape = true;
                    continue;
                }

                if (_q[i] != quoteChar)
                {
                    continue;
                }
                Column += i + 1 - _pos;

                _pos = i + 1;
                _tokenLength = _pos - _tokenStart;

                str = hasEscape ? 
                    GetEscapedString(quoteChar) :
                    new StringSegment(Input, _tokenStart + 1, _tokenLength - 2);

                return true;
            }
            str = default(StringSegment);
            return false;
        }

        private StringSegment GetEscapedString(char quoteChar)
        {
            var sb = new StringBuilder(Input, _tokenStart + 1, _tokenLength - 2, _tokenLength - 2);
            for (int i = 0; i < sb.Length; i++)
            {
                if (sb[i] == quoteChar)
                {
                    sb.Remove(i, 1);
                    continue;
                }

                switch (sb[i])
                {
                    case '\\':
                        if (i + 1 >= sb.Length)
                            goto Fail;

                        switch (sb[i+1])
                        {
                            case '\'':
                            case '"':
                            case '\\':
                                sb.Remove(i, 1);
                                break;
                            case 'T':
                            case 't':
                                sb.Remove(i, 1);
                                sb[i] = '\t';
                                break;
                            case 'N':
                            case 'n':
                                sb.Remove(i, 1);
                                sb[i] = '\n';
                                break;
                            case 'R':
                            case 'r':
                                sb.Remove(i, 1);
                                sb[i] = '\r';
                                break;
                            case 'F':
                            case 'f':
                                sb.Remove(i, 1);
                                sb[i] = '\f';
                                break;
                            case 'B':
                            case 'b':
                                sb.Remove(i, 1);
                                sb[i] = '\b';
                                break;
                            default:
                                goto Fail;
                        }

                        break;
                }
            }

            return sb.ToString();

            Fail:
            throw new QueryParser.ParseException(Column + ":" + Line + " unrecognized escape character found in string in query: '" + Input + "'");

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
                        if (String(out _) == false)
                            goto Failed;
                        // we are now positioned at the _next_character, but we'll increment it
                        // need to go back to stay in the same place :-)
                        _pos--;
                        break;
                    case '/':
                        // Detect // from the second /, we know that there is at least the { before us,
                        // so no need to do range check
                        if(_q[_pos-1] == '/')
                        {
                            for (; _pos < _q.Length; _pos++)
                            {
                                if (_q[_pos] == '\r' || _q[_pos] == '\n')
                                    break;
                            }
                        }
                        break;
                    case '*':
                        // Detect /* from the second *, we know that there is at least the { before us,
                        // so no need to do range check
                        if (_q[_pos - 1] == '/')
                        {
                            for (; _pos < _q.Length; _pos++)
                            {
                                if (_q[_pos] == '*' && _pos +1 < _q.Length && _q[_pos+1] == '/')
                                {
                                    _pos++;
                                    break;
                                }
                            }
                        }
                        break;
                    case '{':
                        nested++;
                        break;
                    case '}':
                        if (--nested == 0)
                        {
                            _pos += 1;
                            _tokenStart = original;
                            _tokenLength = _pos - original;
                            return true;
                        }
                        break;
                }
            }
            Failed:
            _pos = original;
            return false;

        }

        public void GoBack(int matchLength)
        {
            _pos -= matchLength;
            Column -= matchLength;

        }

        public void GoBack()
        {
            GoBack(_tokenLength);
        }
    }
}
