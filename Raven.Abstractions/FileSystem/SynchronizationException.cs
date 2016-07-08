using System;
using System.Runtime.Serialization;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.FileSystem
{
    [Serializable]
    public class SynchronizationException : Exception
    {
        public SynchronizationException()
        {
        }

        public SynchronizationException(string message)
            : base(message)
        { }

        public SynchronizationException(string message, Exception inner)
            : base(message, inner)
        { }

        protected SynchronizationException(SerializationInfo info, StreamingContext context) : base(info, context)
        { }
    }
}
