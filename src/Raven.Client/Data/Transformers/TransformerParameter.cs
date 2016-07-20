using System;

namespace Raven.Client.Data.Transformers
{
    public class TransformerParameter
    {
        public T Value<T>()
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }
    }
}