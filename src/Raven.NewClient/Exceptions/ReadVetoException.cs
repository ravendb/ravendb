//-----------------------------------------------------------------------
// <copyright file="ReadVetoException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;

namespace Raven.NewClient.Client.Exceptions
{
    /// <summary>
    /// This exception is raised whenever a trigger vetoes the read by the session
    /// </summary>
    public class ReadVetoException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ReadVetoException"/> class.
        /// </summary>
        public ReadVetoException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadVetoException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ReadVetoException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadVetoException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ReadVetoException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
