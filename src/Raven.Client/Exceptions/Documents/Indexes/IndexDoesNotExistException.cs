//-----------------------------------------------------------------------
// <copyright file="IndexDoesNotExistException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    /// <summary>
    /// This exception is raised when a query is made against a non existing index
    /// </summary>
    public sealed class IndexDoesNotExistException : RavenException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDoesNotExistException"/> class.
        /// </summary>
        public IndexDoesNotExistException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDoesNotExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public IndexDoesNotExistException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDoesNotExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public IndexDoesNotExistException(string message, Exception inner) : base(message, inner)
        {
        }

        public static IndexDoesNotExistException ThrowFor(string indexName)
        {
            throw new IndexDoesNotExistException($"There is no index with '{indexName}' name.");
        }

        public static IndexDoesNotExistException ThrowForAuto(string indexName)
        {
            throw new IndexDoesNotExistException($"There is no auto index with '{indexName}' name.");
        }
    }
}
