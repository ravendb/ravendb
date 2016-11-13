using System;
using System.Collections.Generic;
using System.Globalization;
using Sparrow.Json;

namespace Raven.NewClient.Client.Documents.Blit
{
    public class BlitPath
    {
        private readonly string _expression;
        public List<object> Parts { get; private set; }

        private int _currentIndex;

        public BlitPath(string expression)
        {
            _expression = expression;
            Parts = new List<object>();

            ParseMain();
        }

        private void ParseMain()
        {
            int currentPartStartIndex = _currentIndex;
            bool followingIndexer = false;

            while (_currentIndex < _expression.Length)
            {
                char currentChar = _expression[_currentIndex];

                switch (currentChar)
                {
                    case '[':
                    case '(':
                        if (_currentIndex > currentPartStartIndex)
                        {
                            string member = _expression.Substring(currentPartStartIndex, _currentIndex - currentPartStartIndex);
                            Parts.Add(member);
                        }

                        ParseIndexer(currentChar);
                        currentPartStartIndex = _currentIndex + 1;
                        followingIndexer = true;
                        break;
                    case ']':
                    case ')':
                        throw new Exception("Unexpected character while parsing path: " + currentChar);
                    case '.':
                        if (_currentIndex > currentPartStartIndex)
                        {
                            string member = _expression.Substring(currentPartStartIndex, _currentIndex - currentPartStartIndex);
                            Parts.Add(member);
                        }
                        currentPartStartIndex = _currentIndex + 1;
                        followingIndexer = false;
                        break;
                    default:
                        if (followingIndexer)
                            throw new Exception("Unexpected character following indexer: " + currentChar);
                        break;
                }

                _currentIndex++;
            }

            if (_currentIndex > currentPartStartIndex)
            {
                string member = _expression.Substring(currentPartStartIndex, _currentIndex - currentPartStartIndex);
                Parts.Add(member);
            }
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

        internal object Evaluate(BlittableJsonReaderBase root, bool errorWhenNoMatch)
        {
            object current = root;

            foreach (object part in Parts)
            {
                var propertyName = part as string;
                if (propertyName != null)
                {
                    var o = current as BlittableJsonReaderObject;
                    if (o != null)
                    {
                      
                            var newProp = o[propertyName];
                            if (newProp != null)
                            {
                                current = o[propertyName];
                            }
                            else
                            {
                                current = null;
                            }

                        if (current == null && errorWhenNoMatch)
                            string.Format(CultureInfo.InvariantCulture, "Property '{0}' does not exist on JSON.", propertyName);
                        
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
                                    if (errorWhenNoMatch)
                                        string.Format(CultureInfo.InvariantCulture, "Property '{0}' not valid on {1}.", current.GetType().Name);
                                    
                                    break;
                            }
                            continue;
                        }
                        if (errorWhenNoMatch)
                            string.Format(CultureInfo.InvariantCulture, "Property '{0}' not valid on {1}.", current.GetType().Name);
                        

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
                            if (errorWhenNoMatch)
                                string.Format(CultureInfo.InvariantCulture, "Index {0} outside the bounds of JSON.", index);

                            return null;
                        }

                        current = a[index];
                    }
                    else
                    {
                        var b = current as BlittableJsonReaderObject;


                        if (a.Length <= index)
                        {
                            if (errorWhenNoMatch)
                                string.Format(CultureInfo.InvariantCulture, "Index {0} outside the bounds of JSON.", index);

                            return null;
                        }
                        var trueIndex = b.GetPropertiesByInsertionOrder()[index];
                        var prop = new BlittableJsonReaderObject.PropertyDetails();

                        b.GetPropertyByIndex(trueIndex, ref prop);

                        current = prop.Value;
                    }
                }
            }

            return current;
        }
    }

}
