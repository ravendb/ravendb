using System;
using System.Runtime.Serialization;

namespace Raven.Database.Exceptions
{
    [Serializable]
    public class ConcurrencyException : Exception
    {
        public Guid ExpectedETag { get; set; }
        public Guid ActualETag { get; set; }

        public ConcurrencyException()
        {
        }

        public ConcurrencyException(string message) : base(message)
        {
        }

        public ConcurrencyException(string message, Exception inner) : base(message, inner)
        {
        }

        protected ConcurrencyException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}