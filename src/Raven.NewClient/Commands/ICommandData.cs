//-----------------------------------------------------------------------
// <copyright file="ICommandData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.NewClient.Json.Linq;

namespace Raven.Abstractions.Commands
{
    /// <summary>
    /// A single operation inside a batch
    /// </summary>
    public interface ICommandData
    {
        /// <summary>
        /// Key of a document.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Returns operation method.
        /// </summary>
        string Method { get; }

        /// <summary>
        /// Current document etag, used for concurrency checks (null to skip check)
        /// </summary>
        long? Etag { get; }

        /// <summary>
        /// Additional command data. For internal use only.
        /// </summary>
        RavenJObject AdditionalData { get; set; }

        /// <summary>
        /// Translates this instance to a Json object.
        /// </summary>
        /// <returns>RavenJObject representing the command.</returns>
        RavenJObject ToJson();
    }
}
