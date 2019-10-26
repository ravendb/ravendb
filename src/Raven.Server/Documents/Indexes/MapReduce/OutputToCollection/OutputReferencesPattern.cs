using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReferencesPattern
    {
        private static readonly Regex FieldsRegex = new Regex(@"\{([^\:}]*)\:?([^}]*)\}", RegexOptions.Compiled);

        private readonly DocumentIdBuilder _builder;

        public OutputReferencesPattern(string pattern)
        {
            var matches = FieldsRegex.Matches(pattern);

            if (matches.Count == 0)
                throw new InvalidOperationException("Provided pattern is not supported: " + pattern);
            
            var fieldToFormatPosition = new Dictionary<string, int>(matches.Count);
            int numberOfFields = 0;

            var formattedPattern = FieldsRegex.Replace(pattern, StringFormatEvaluator);

            _builder = new DocumentIdBuilder(formattedPattern, fieldToFormatPosition);

            string StringFormatEvaluator(Match m)
            {
                var groups = m.Groups;

                try
                {
                    if (groups.Count == 2)
                    {
                        // {OrderedAt}

                        return $"{{{numberOfFields}}}";
                    }

                    if (groups.Count == 3)
                    {
                        // {OrderedAt:yyyy-MM-dd}

                        return $"{{{numberOfFields}:{groups[2]}}}";
                    }
                }
                finally
                {
                    fieldToFormatPosition.Add(groups[1].ToString(), numberOfFields);
                    numberOfFields++;
                }
                
                throw new NotSupportedException("Provided pattern is not supported: " + pattern);
            }
        }

        public IDisposable BuildReferenceDocumentId(out DocumentIdBuilder builder)
        {
            builder = _builder;

            return _builder;
        }

        public class DocumentIdBuilder : IDisposable
        {
            private readonly string _formattedPattern;
            private readonly Dictionary<string, int> _fieldToFormatPosition;
            private readonly StringBuilder _id;
            private readonly object[] _values;
            private readonly int _countOfFields;
            private int _addedFields;

            public DocumentIdBuilder(string formattedPattern, Dictionary<string, int> fieldToFormatPosition)
            {
                _formattedPattern = formattedPattern;
                _fieldToFormatPosition = fieldToFormatPosition;
                _countOfFields = _fieldToFormatPosition.Count;
                _values = new object[_countOfFields];
                _id = new StringBuilder(formattedPattern.Length);
            }

            public bool ContainsField(string fieldName)
            {
                return _fieldToFormatPosition.ContainsKey(fieldName);
            }

            public string GetId()
            {
                if (_addedFields != _countOfFields)
                    throw new InvalidOperationException($"Cannot create identifier for reference document of reduce outputs. Expected to process {_countOfFields} fields while it got {_addedFields}.");

                return _id.AppendFormat(_formattedPattern, _values).ToString();
            }

            public void Dispose()
            {
                _id.Clear();
                _addedFields = 0;
            }

            public void Add(string fieldName, object fieldValue)
            {
                var pos = _fieldToFormatPosition[fieldName];

                _values[pos] = fieldValue;

                _addedFields++;
            }
        }

        public HashSet<string> FieldNames { get; }
        
        public List<PatternElement> Parts = new List<PatternElement>();

        public class PatternElement
        {
            public ElementType Type;
            public string StringValue;
            public string FieldReplacementVariable;
            public string FieldName;
            public string FieldFormatting;
        }

        public enum ElementType
        {
            None = 0,
            StringConst = 1,
            Field = 2
        }
    }
}
