using System;

namespace Sparrow.Json
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    internal sealed class JsonDeserializationStringDictionaryAttribute : Attribute
    {
        public readonly StringComparison StringComparison;

        public JsonDeserializationStringDictionaryAttribute(StringComparison stringComparison)
        {
            StringComparison = stringComparison;
        }
    }
}
