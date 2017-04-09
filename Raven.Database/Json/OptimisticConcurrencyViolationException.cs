using System;

namespace Raven.Database.Json
{
    public class OptimisticConcurrencyViolationException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OptimisticConcurrencyViolationException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public OptimisticConcurrencyViolationException(string message) : base(message)
        {
        }
    }
}