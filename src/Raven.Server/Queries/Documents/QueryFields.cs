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
        public readonly Dictionary<string, (string Value, ValueTokenType ValueType)> Fields = new Dictionary<string, (string Value, ValueTokenType ValueType)>();

        public (string Value, ValueTokenType ValueType) this[string name]
        {
            get
            {
                return Fields[name];
            }
        }

        public void Add(string fieldName, string value, ValueTokenType valueType)
        {
            Fields[fieldName] = (value, valueType);
        }
    }
}