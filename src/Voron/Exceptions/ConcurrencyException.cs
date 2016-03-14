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
        public long ExpectedETag { get; set; }

        /// <summary>
        /// Actual Etag.
        /// </summary>
        public long ActualETag { get; set; }
    }
}
