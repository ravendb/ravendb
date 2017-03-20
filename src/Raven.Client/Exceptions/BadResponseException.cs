//-----------------------------------------------------------------------
// <copyright file="BadResponseException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions
{
    /// <summary>
    /// This exception is raised when a bad response is send from the server
    /// </summary>
    public class BadResponseException : RavenException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BadResponseException"/> class.
        /// </summary>
        public BadResponseException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BadResponseException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public BadResponseException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BadResponseException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public BadResponseException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
