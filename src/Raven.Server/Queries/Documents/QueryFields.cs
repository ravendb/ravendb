using System.Collections.Generic;
using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries
{
    public class QueryFields
    {
        public WhereFields Where;

        public List<(string Name, OrderByFieldType OrderingType, bool Ascending)> OrderBy;
    }

    public class WhereFields
    {
        public readonly List<string> AllFieldNames = new List<string>();

        public readonly Dictionary<string, (string Value, ValueTokenType ValueType)> SingleValueFields = new Dictionary<string, (string Value, ValueTokenType ValueType)>();

        public readonly Dictionary<string, (List<string> Values, ValueTokenType ValueType)> MultipleValuesFields = new Dictionary<string, (List<string> Value, ValueTokenType ValueType)>();

        public void Add(string fieldName, (string, ValueTokenType) value)
        {
            SingleValueFields[fieldName] = value;
            AllFieldNames.Add(fieldName);
        }

        public void Add(string fieldName, List<string> values, ValueTokenType valuesType)
        {
            MultipleValuesFields[fieldName] = (values, valuesType);
            AllFieldNames.Add(fieldName);
        }
    }
}