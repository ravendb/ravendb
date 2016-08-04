using System;

namespace Raven.Client.Data.Transformers
{
    public class TransformerParameter
    {
        internal const string Prefix = "tp-";

        public virtual T Value<T>()
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }
    }
}