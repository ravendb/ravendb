//-----------------------------------------------------------------------
// <copyright file="IndexDisabledException.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Serialization;
using Raven.Abstractions.Data;
using Raven.Client.Data;

namespace Raven.Abstractions.Exceptions
{
    /// <summary>
    /// This exception is raised when querying an index that was disabled because the error rate exceeded safety margins
    /// </summary>
    public class IndexDisabledException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
        /// </summary>
        public IndexDisabledException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
        /// </summary>
        /// <param name="information">The information.</param>
        public IndexDisabledException(IndexFailureInformation information)
        {
            Information = information;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public IndexDisabledException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDisabledException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public IndexDisabledException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// Information about the index failure .
        /// </summary>
        public IndexFailureInformation Information { get; set; }
    }
}
