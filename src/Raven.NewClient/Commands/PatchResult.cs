//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.NewClient.Client.Commands
{
    /// <summary>
    /// The result of a PatchResult operation
    /// </summary>
    public class PatchResult
    {
        /// <summary>
        /// Id of patch operation.
        /// </summary>
        public int OperationId { get; set; }

    }
}
