using System;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerParameter
    {
        public TransformerParameter(object value)
        {
            OriginalValue = value;
        }

        public readonly object OriginalValue;

        public T Value<T>()
        {
            return TypeConverter.Convert<T>(OriginalValue, cast: false);
        }
    }
}