//-----------------------------------------------------------------------
// <copyright file="TransformerDoesNotExistException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Exceptions.Transformers
{
    /// <summary>
    /// This exception is raised when a query is made against a non existing index
    /// </summary>
    public class TransformerDoesNotExistException : RavenException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransformerDoesNotExistException"/> class.
        /// </summary>
        public TransformerDoesNotExistException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransformerDoesNotExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public TransformerDoesNotExistException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransformerDoesNotExistException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public TransformerDoesNotExistException(string message, Exception inner) : base(message, inner)
        {
        }

        public static TransformerDoesNotExistException ThrowFor(string transformerName)
        {
            throw new TransformerDoesNotExistException($"There is no transformer with '{transformerName}' name.");
        }
    }
}
