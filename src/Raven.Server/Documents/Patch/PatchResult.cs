//-----------------------------------------------------------------------
// <copyright file="PatchResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;

namespace Raven.Server.Documents.Patch
{
    /// <summary>
    /// The result of a patch operation
    /// </summary>
    public enum PatchResult
    {
        /// <summary>
        /// The document does not exists, operation was a no-op
        /// </summary>
        DocumentDoesNotExists,
        /// <summary>
        /// Document was properly patched
        /// </summary>
        Patched,
        /// <summary>
        /// Document was properly tested
        /// </summary>
        Tested,
        /// <summary>
        /// The document was not patched, because skipPatchIfEtagMismatch was set
        /// and the etag did not match
        /// </summary>
        Skipped,
        /// <summary>
        /// Neither document body not metadata was changed during patch operation
        /// </summary>
        NotModified
    }

    public class PatchResultData
    {
        /// <summary>
        /// Result of patch operation:
        /// <para>- DocumentDoesNotExists - document does not exists, operation was a no-op,</para>
        /// <para>- Patched - document was properly patched,</para>
        /// <para>- Tested - document was properly tested,</para>
        /// <para>- Skipped - document was not patched, because skipPatchIfEtagMismatch was set and the etag did not match,</para>
        /// <para>- NotModified - neither document body not metadata was changed during patch operation</para>
        /// </summary>
        public PatchResult PatchResult { get; set; }

        /// <summary>
        /// Patched document.
        /// </summary>
        public BlittableJsonReaderObject ModifiedDocument { get; set; }

        public BlittableJsonReaderObject OriginalDocument { get; set; }

        /// <summary>
        /// Additional debugging information (if requested).
        /// </summary>
        public DynamicJsonValue DebugActions { get; set; }

        public DynamicJsonArray DebugInfo { get; set; }
    }
}
