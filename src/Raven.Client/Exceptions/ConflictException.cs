//-----------------------------------------------------------------------
// <copyright file="ConflictException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Exceptions
{
    /// <summary>
    /// This exception occurs when a (replication) conflict is encountered.
    /// Usually this required a user to manually resolve the conflict.
    /// </summary>
    public abstract class ConflictException : RavenException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConflictException"/> class.
        /// </summary>
        protected ConflictException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConflictException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        protected ConflictException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConflictException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        protected ConflictException(string message, Exception inner)
            : base(message, inner)
        {
        }

    }
}
