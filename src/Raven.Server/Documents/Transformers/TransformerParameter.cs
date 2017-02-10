using Raven.Server.Utils;

namespace Raven.Server.Documents.Transformers
{
    public class TransformerParameter : Client.Data.Transformers.TransformerParameter
    {
        public TransformerParameter(object value)
        {
            OriginalValue = value;
        }

        public readonly object OriginalValue;

        public override T Value<T>()
        {
            return TypeConverter.Convert<T>(OriginalValue, cast: false);
        }
    }
}