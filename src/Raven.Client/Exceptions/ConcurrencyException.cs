//-----------------------------------------------------------------------
// <copyright file="ConcurrencyException.cs" company="Hibernating Rhinos LTD">
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
    public class ConcurrencyException : ConflictException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
        /// </summary>
        public ConcurrencyException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        public ConcurrencyException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrencyException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="inner">The inner.</param>
        public ConcurrencyException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// Expected Etag.
        /// </summary>
        public long ExpectedETag;

        /// <summary>
        /// Actual Etag.
        /// </summary>
        public long ActualETag;

        /// <summary>
        /// Expected Change Vector.
        /// </summary>
        public string ExpectedChangeVector;

        /// <summary>
        /// Actual Change Vector.
        /// </summary>
        public string ActualChangeVector;

        /// <summary>
        /// Cluster Transaction conflicts info.
        /// </summary>
        public Conflict[] ClusterTransactionConflicts;

        public class Conflict
        {
            public ConflictType Type;
            public string Id;
            public string Expected;
            public string Actual;

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
