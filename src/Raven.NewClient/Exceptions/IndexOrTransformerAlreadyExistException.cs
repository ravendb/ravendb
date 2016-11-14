//-----------------------------------------------------------------------
// <copyright file="IndexAlreadyExistsException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.NewClient.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised if creation of index is attempted when there is already an index with identical name
    /// </summary>
    public class IndexOrTransformerAlreadyExistException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IndexOrTransformerAlreadyExistException"/> class.
        /// </summary>
        public IndexOrTransformerAlreadyExistException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexOrTransformerAlreadyExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public IndexOrTransformerAlreadyExistException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexOrTransformerAlreadyExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public IndexOrTransformerAlreadyExistException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
