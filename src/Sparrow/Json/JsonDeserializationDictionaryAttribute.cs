using System;

namespace Sparrow.Json
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    internal sealed class JsonDeserializationDictionaryAttribute : Attribute
    {
        public readonly StringComparison StringComparison;

        public JsonDeserializationDictionaryAttribute(StringComparison stringComparison)
        {
            StringComparison = stringComparison;
        }
    }
}
