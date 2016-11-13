//-----------------------------------------------------------------------
// <copyright file="IndexDoesNotExistsException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised when a query is made against a non existing index
    /// </summary>
    public class IndexDoesNotExistsException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDoesNotExistsException"/> class.
        /// </summary>
        public IndexDoesNotExistsException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDoesNotExistsException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public IndexDoesNotExistsException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDoesNotExistsException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public IndexDoesNotExistsException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
