//-----------------------------------------------------------------------
// <copyright file="PutResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Commands.Batches
{
    /// <summary>
    /// The result of a PUT operation
    /// </summary>
    public class PutResult
    {
        /// <summary>
        /// Id of the document that was PUT.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// long? of the document after PUT operation.
        /// </summary>
        public long? ETag { get; set; }
    }
}