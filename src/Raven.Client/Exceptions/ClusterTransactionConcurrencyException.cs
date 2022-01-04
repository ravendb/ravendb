//-----------------------------------------------------------------------
// <copyright file="ClusterTransactionConcurrencyException .cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Exceptions
{
    /// <summary>
    /// This exception is raised when a concurrency conflict is encountered
    /// </summary>
    public class ClusterTransactionConcurrencyException : ConcurrencyException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterTransactionConcurrencyException "/> class.
        /// </summary>
        public ClusterTransactionConcurrencyException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterTransactionConcurrencyException "/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ClusterTransactionConcurrencyException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterTransactionConcurrencyException "/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ClusterTransactionConcurrencyException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// Concurrency violations info.
        /// </summary>
        public Conflict[] ConcurrencyViolations { get; set; }

        public class Conflict
        {
            public ConflictType Type { get; set; }
            public string Id { get; set; }
            public string Expected { get; set; }
            public string Actual { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue()
                {
                    [nameof(Type)] = Type,
                    [nameof(Id)] = Id,
                    [nameof(Expected)] = Expected,
                    [nameof(Actual)] = Actual
                };
            }
        }

        public enum ConflictType
        {
            Document,
            CompareExchange
        }
    }
}
