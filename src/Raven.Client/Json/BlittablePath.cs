using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Json;

namespace Raven.Client.Json
{
    internal class BlittablePath
    {
        private readonly string _expression;
        public List<object> Parts { get; }

        private int _currentIndex;

        public BlittablePath(string expression)
        {
            _expression = expression;
            Parts = new List<object>();

            ParseMain();
        }

        private static readonly char[] _escapeChars = {'[', ']', '(', ')', '.'};
        public static string EscapeString(string str)
        {
            StringBuilder sb = null;
            for (int i = 0; i< str.Length; i++)
            {
                char c = str[i];
                //We need to escape
                if (IsEscapeChar(c))
                {
                    //First time should append prefix
                    if (sb == null)
                    {
                        sb = new StringBuilder(str.Length,str.Length*2);
                        sb.Append(str, 0, i);
                    }
                    sb.Append('\\');                    
                }

                if (sb != null)
                {
                    sb.Append(c);
                }
            }

            return sb == null ? str : sb.ToString();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsEscapeChar(char c)
        {
            return _escapeChars[0] == c ||
                   _escapeChars[1] == c ||
                   _escapeChars[2] == c ||
                   _escapeChars[3] == c ||
                   _escapeChars[4] == c;
        }

        private void ParseMain()
        {
            int currentPartStartIndex = _currentIndex;
            bool followingIndexer = false;
            bool escapeChar = false;
            Lazy<List<int>> escapePositions = new Lazy<List<int>>(()=> new List<int>());

            while (_currentIndex < _expression.Length)
            {
                char currentChar = _expression[_currentIndex];

                switch (currentChar)
                {
                    case '[':
                    case '(':
                        if (escapeChar)
                        {
                            escapePositions.Value.Add(_currentIndex);
                            escapeChar = false;
                            break;
                        }
                        if (_currentIndex > currentPartStartIndex)
                        {
                            
                            string member = GetEscapedMember(_expression, currentPartStartIndex, _currentIndex - currentPartStartIndex, escapePositions.Value);
                            Parts.Add(member);
                        }

                        ParseIndexer(currentChar);
                        currentPartStartIndex = _currentIndex + 1;
                        followingIndexer = true;
                        break;
                    case ']':
                    case ')':
                        if (escapeChar)
                        {
                            escapePositions.Value.Add(_currentIndex);
                            escapeChar = false;
                            break;
                        }
                        throw new Exception("Unexpected character while parsing path: " + currentChar);
                    case '.':
                        if (escapeChar)
                        {
                            escapePositions.Value.Add(_currentIndex);
                            escapeChar = false;
                            break;
                        }
                        if (_currentIndex > currentPartStartIndex)
                        {
                            string member = GetEscapedMember(_expression,currentPartStartIndex, _currentIndex - currentPartStartIndex, escapePositions.Value);
                            Parts.Add(member);
                        }
                        currentPartStartIndex = _currentIndex + 1;
                        followingIndexer = false;
                        break;
                    case '\\':
                        escapeChar = true;
                        break;
                    default:
                        if (followingIndexer)
                            throw new Exception("Unexpected character following indexer: " + currentChar);
                        escapeChar = false;
                        break;
                }

                _currentIndex++;
            }

            if (_currentIndex > currentPartStartIndex)
            {
                string member = GetEscapedMember(_expression, currentPartStartIndex, _currentIndex - currentPartStartIndex, escapePositions.Value);
                Parts.Add(member);
            }
        }

        private string GetEscapedMember(string expression, int start, int length, List<int> escapePositions)
        {
            if (escapePositions.Count == 0)
            {
                return expression.Substring(start, length);
            }
            var sb = new StringBuilder(length);
            var startPos = start;
            var remLength = length;
            foreach (var pos in escapePositions)
            {
                var chunkLength = pos - startPos;
                sb.Append(expression, startPos, chunkLength - 1);
                startPos += chunkLength;
                remLength -= chunkLength;
            }
            sb.Append(expression, startPos, remLength);
            escapePositions.Clear();
            return sb.ToString();
        }

        private void ParseIndexer(char indexerOpenChar)
        {
            _currentIndex++;

            char indexerCloseChar = (indexerOpenChar == '[') ? ']' : ')';
            int indexerStart = _currentIndex;
            int indexerLength = 0;
            bool indexerClosed = false;

            while (_currentIndex < _expression.Length)
            {
                char currentCharacter = _expression[_currentIndex];
                if (char.IsDigit(currentCharacter))
                {
                    indexerLength++;
                }
                else if (currentCharacter == indexerCloseChar)
                {
                    indexerClosed = true;
                    break;
                }
                else
                {
                    throw new Exception("Unexpected character while parsing path indexer: " + currentCharacter);
                }

                _currentIndex++;
            }

            if (!indexerClosed)
                throw new Exception("Path ended with open indexer. Expected " + indexerCloseChar);

            if (indexerLength == 0)
                throw new Exception("Empty path indexer.");

            string indexer = _expression.Substring(indexerStart, indexerLength);
            Parts.Add(Convert.ToInt32(indexer, CultureInfo.InvariantCulture));
        }

        internal object Evaluate(BlittableJsonReaderBase root)
        {
            object current = root;

            foreach (var part in Parts)
            {
                var propertyName = part as string;
                if (propertyName != null)
                {
                    var o = current as BlittableJsonReaderObject;
                    if (o != null)
                    {
                        if (o.TryGet(propertyName, out current) == false)
                            current = null;

                        if (current == null)
                            return null;
                    }
                    else
                    {
                        var array = current as BlittableJsonReaderArray;
                        if (array != null)
                        {
                            switch (propertyName)
                            {
                                case "Count":
                                case "count":
                                case "Length":
                                case "length":
                                    current = array.Length;
                                    break;
                                default:
                                    return null;
                            }
                            continue;
                        }

                        return null;
                    }
                }
                else
                {
                    var index = (int)part;


                    var a = current as BlittableJsonReaderArray;

                    if (a != null)
                    {
                        if (a.Length <= index)
                        {
                            return null;
                        }

                        current = a[index];
                    }
                    else
                    {
                        return null;
                    }
                }
            }

            return current;
        }
    }

}
