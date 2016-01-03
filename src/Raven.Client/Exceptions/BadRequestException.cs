//-----------------------------------------------------------------------
// <copyright file="BadRequestException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised when a bad request is made to the server
    /// </summary>
    public class BadRequestException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BadRequestException"/> class.
        /// </summary>
        public BadRequestException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BadRequestException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public BadRequestException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BadRequestException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public BadRequestException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
