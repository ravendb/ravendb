using System;
using System.Runtime.Serialization;

namespace Corax
{
    [Serializable]
    internal sealed class IndexOpenException : Exception
    {
        public IndexOpenException()
        {
        }

        public IndexOpenException(string message) : base(message)
        {
        }

        public IndexOpenException(string message, Exception innerException) : base(message, innerException)
        {
        }

        private IndexOpenException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
