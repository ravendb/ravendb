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
        public string ExpectedChangeVector { get; set; }

        /// <summary>
        /// Actual Etag.
        /// </summary>
        public string ActualChangeVector { get; set; }
    }
}
