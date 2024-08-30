using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Client;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public sealed class OutputReferencesPattern
    {
        public string ReferencesCollectionName { get; }
        public string Pattern => _builder?.Pattern;

        private static readonly Regex FieldsRegex = new Regex(@"\{([^\:}]*)\:?([^}]*)\}", RegexOptions.Compiled);

        private readonly DocumentIdBuilder _builder;

        public OutputReferencesPattern(DocumentDatabase database, string pattern, string referencesCollectionName = null)
        {
            ReferencesCollectionName = referencesCollectionName;

            var fieldToFormatPosition = ValidatePattern(pattern, out var formattedPattern);
            _builder = new DocumentIdBuilder(database, pattern, formattedPattern, fieldToFormatPosition);
        }

        public static Dictionary<string, int> ValidatePattern(string pattern, out string formattedPattern)
        {
            var matches = FieldsRegex.Matches(pattern);

            if (matches.Count == 0)
                throw new IndexInvalidException("Provided pattern is not supported: " + pattern);

            var fieldToFormatPosition = new Dictionary<string, int>(matches.Count);
            int numberOfFields = 0;

            formattedPattern = FieldsRegex.Replace(pattern, StringFormatEvaluator);

            return fieldToFormatPosition;

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
                    string fieldName = groups[1].ToString();

                    if (fieldToFormatPosition.ContainsKey(fieldName))
                        throw new IndexInvalidException($"Pattern should contain unique fields only. Duplicated field: '{fieldName}'");

                    fieldToFormatPosition.Add(fieldName, numberOfFields);
                    numberOfFields++;
                }

                throw new IndexInvalidException("Provided pattern is not supported: " + pattern);
            }
        }

        public IDisposable BuildReferenceDocumentId(out DocumentIdBuilder builder)
        {
            builder = _builder;

            return _builder;
        }

        public sealed class DocumentIdBuilder : IDisposable
        {
            private readonly DocumentDatabase _database;
            private readonly string _pattern;
            private readonly string _formattedPattern;
            private readonly Dictionary<string, int> _fieldToFormatPosition;
            private readonly StringBuilder _id;
            private readonly object[] _values;
            private readonly int _countOfFields;
            private int _addedFields;

            public string Pattern => _pattern;

            public Dictionary<string, int>.KeyCollection PatternFields => _fieldToFormatPosition.Keys;

            public DocumentIdBuilder(DocumentDatabase database, string pattern, string formattedPattern, Dictionary<string, int> fieldToFormatPosition)
            {
                _database = database;
                _pattern = pattern;
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
                    ThrowNumberOfProcessedFieldsMismatch();

                var id = _id.AppendFormat(_formattedPattern, _values).ToString();

                ValidateId(id);

                return id;
            }

            public void ValidateId(string id)
            {
                var identityPartsSeparator = _database?.IdentityPartsSeparator ?? Constants.Identities.DefaultSeparator;

                if (id.EndsWith(identityPartsSeparator))
                    ThrowInvalidId(id, $"reference ID must not end with '{identityPartsSeparator}' character");

                if (id.EndsWith('|'))
                    ThrowInvalidId(id, "reference ID must not end with '|' character");
            }

            public void Dispose()
            {
                _id.Clear();
                _addedFields = 0;
            }

            public void Add(string fieldName, object fieldValue)
            {
                if (fieldValue == null || fieldValue is DynamicNullObject)
                    ThrowEncounteredNullValueInPattern(fieldName);

                var pos = _fieldToFormatPosition[fieldName];

                _values[pos] = fieldValue;

                _addedFields++;
            }

            [DoesNotReturn]
            private void ThrowNumberOfProcessedFieldsMismatch()
            {
                throw new InvalidOperationException(
                    $"Cannot create identifier for reference document of reduce outputs. Expected to process {_countOfFields} fields while it got {_addedFields}. Pattern: '{_pattern}'");
            }

            [DoesNotReturn]
            private static void ThrowInvalidId(string id, string message)
            {
                throw new InvalidOperationException($"Invalid pattern reference document ID: '{id}'. Error: {message}");
            }

            [DoesNotReturn]
            private static void ThrowEncounteredNullValueInPattern(string fieldName)
            {
                throw new InvalidOperationException($"Invalid pattern reference document ID. Field '{fieldName}' was null");
            }
        }
    }
}
