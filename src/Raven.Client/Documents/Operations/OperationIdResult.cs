//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Operations
{
    /// <summary>
    /// The result of a OperationIdResult operation
    /// </summary>
    public class OperationIdResult
    {
        /// <summary>
        /// Id of patch operation.
        /// </summary>
        public long OperationId { get; set; }

    }
}
