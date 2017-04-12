using System;
using System.Runtime.Serialization;

namespace Raven.Database.Json
{
    [Serializable]
    public class OptimisticConcurrencyViolationException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OptimisticConcurrencyViolationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public OptimisticConcurrencyViolationException(string message) : base(message)
        {
        }

        protected OptimisticConcurrencyViolationException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context)
        {
        }
    }
}