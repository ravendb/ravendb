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
    /// This exception is raised when a patch is made against a non existing document
    /// </summary>
    public class DocumentDoesNotExistsException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentDoesNotExistsException"/> class.
        /// </summary>
        public DocumentDoesNotExistsException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentDoesNotExistsException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public DocumentDoesNotExistsException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentDoesNotExistsException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public DocumentDoesNotExistsException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
