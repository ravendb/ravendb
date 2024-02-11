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
    public sealed class ClusterTransactionConcurrencyException : ConcurrencyException
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
        public ConcurrencyViolation[] ConcurrencyViolations { get; set; }

        public sealed class ConcurrencyViolation
        {
            /// <summary>
            /// Concurrency violation occured on <see cref="ViolationOnType"/>
            /// </summary>
            public ViolationOnType Type { get; set; }

            /// <summary>
            /// The ID of which the concurrency check failed 
            /// </summary>
            public string Id { get; set; }

            /// <summary>
            /// The expected index for the concurrency check
            /// </summary>
            public long Expected { get; set; }

            /// <summary>
            /// The actual index of the concurrency check
            /// </summary>
            public long Actual { get; set; }

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

        public enum ViolationOnType
        {
            /// <summary>
            /// Concurrency violation occured on a document
            /// </summary>
            Document,
            /// <summary>
            /// Concurrency violation occured on a compare exchange
            /// </summary>
            CompareExchange
        }
    }
}
