using System;
using System.Runtime.Serialization;

namespace Raven.Client.Exceptions
{
    [Serializable]
    public class ConflictException : Exception
    {
        public string[] ConflictedVersionIds { get; set; }

        public ConflictException()
        {
        }

        public ConflictException(string message) : base(message)
        {
        }

        public ConflictException(string message, Exception inner) : base(message, inner)
        {
        }

        protected ConflictException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}