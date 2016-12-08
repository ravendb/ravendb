//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.NewClient.Client.Connection;

namespace Raven.NewClient.Client.Commands
{
    /// <summary>
    /// The result of a PatchResult operation
    /// </summary>
    public class DeleteResult
    {
        /// <summary>
        /// Id of patch operation.
        /// </summary>
        public int OperationId { get; set; }

    }
}
