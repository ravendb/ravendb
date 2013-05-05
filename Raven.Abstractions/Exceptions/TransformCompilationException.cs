using System;
using System.Runtime.Serialization;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Exceptions
{
    public class TransformCompilationException : Exception
    {
        public TransformCompilationException(string message) : base(message)
        {
        }

        public TransformCompilationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
