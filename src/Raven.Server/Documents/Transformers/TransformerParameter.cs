using System;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerParameter
    {
        private readonly object _value;

        public TransformerParameter(object value)
        {
            _value = value;
        }

        public T Value<T>()
        {
            return TypeConverter.Convert<T>(_value, cast: false);
        }
    }
}