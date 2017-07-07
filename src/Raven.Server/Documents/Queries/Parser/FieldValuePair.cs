namespace Raven.Server.Documents.Queries.Parser
{
    public struct FieldValuePair
    {
        public readonly string Name;
        public readonly string Value;
        public readonly ValueTokenType ValueType;

        public FieldValuePair(string name, string value, ValueTokenType valueType)
        {
            Name = name;
            Value = value;
            ValueType = valueType;
        }
    }
}