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
        /// Id of operation.
        /// </summary>
        public long OperationId { get; set; }

        /// <summary>
        /// Node tag of operation.
        /// </summary>
        public string OperationNodeTag { get; set; }

    }
}
