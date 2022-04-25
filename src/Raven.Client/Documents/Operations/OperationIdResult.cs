//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Client.Documents.Operations
{
    public class OperationIdResult<TResult> : OperationIdResult
    {
        public TResult Result { get; set; }
    }

    /// <summary>
    /// The result of a OperationIdResult operation
    /// </summary>
    public class OperationIdResult
    {
        /// <summary>
        /// Id of operation.
        /// </summary>
        public long OperationId { get; set; }

        /// <summary>
        /// Node tag of operation.
        /// </summary>
        public string OperationNodeTag { get; set; }

        internal OperationIdResult<TResult> ForResult<TResult>(TResult result)
        {
            return new OperationIdResult<TResult>
            {
                OperationId = OperationId,
                OperationNodeTag = OperationNodeTag,
                Result = result
            };
        }
    }
}
