//-----------------------------------------------------------------------
// <copyright file="DocumentDoesNotExistException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised when server is not supported version.
    /// </summary>
    [Serializable]
    public class ServerVersionNotSuppportedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServerVersionNotSuppportedException"/> class.
        /// </summary>
        public ServerVersionNotSuppportedException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerVersionNotSuppportedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ServerVersionNotSuppportedException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ServerVersionNotSuppportedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ServerVersionNotSuppportedException(string message, Exception inner) : base(message, inner)
        {
        }

        protected ServerVersionNotSuppportedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
