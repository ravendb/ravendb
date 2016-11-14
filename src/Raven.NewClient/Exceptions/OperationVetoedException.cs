//-----------------------------------------------------------------------
// <copyright file="OperationVetoedException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.NewClient.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised when an operation has been vetoed by a trigger
    /// </summary>
    public class OperationVetoedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
        /// </summary>
        public OperationVetoedException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public OperationVetoedException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="OperationVetoedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public OperationVetoedException(string message, Exception inner) : base(message, inner)
        {
        }

    }
}
