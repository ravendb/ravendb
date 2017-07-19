using Sparrow.Json;

namespace Voron.Exceptions
{
    using System;

    public class ConcurrencyException : Exception
    {
        public ConcurrencyException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Expected Etag.
        /// </summary>
        public LazyStringValue ExcpectedChangeVector { get; set; }

        /// <summary>
        /// Actual Etag.
        /// </summary>
        public LazyStringValue ActualChangeVector { get; set; }
    }
}
