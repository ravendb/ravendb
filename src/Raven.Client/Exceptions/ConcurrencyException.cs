//-----------------------------------------------------------------------
// <copyright file="ConcurrencyException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions
{
    /// <summary>
    /// This exception is raised when a concurrency conflict is encountered
    /// </summary>
    public class ConcurrencyException : ConflictException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
        /// </summary>
        public ConcurrencyException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ConcurrencyException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ConcurrencyException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// Expected Etag.
        /// </summary>
        [Obsolete("Not used and will be removed and the next major version")]
        public long ExpectedETag { get; set; }

        /// <summary>
        /// Actual Etag.
        /// </summary>
        [Obsolete("Not used and will be removed and the next major version")]
        public long ActualETag { get; set; }

        /// <summary>
        /// Expected Change Vector.
        /// </summary>
        public string ExpectedChangeVector { get; set; }

        /// <summary>
        /// Actual Change Vector.
        /// </summary>
        public string ActualChangeVector { get; set; }

        /// <summary>
        /// The Document Id.
        /// </summary>
        public string Id { get; set; }
    }
}
