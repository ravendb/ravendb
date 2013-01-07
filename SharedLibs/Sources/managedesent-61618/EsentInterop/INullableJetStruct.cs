//-----------------------------------------------------------------------
// <copyright file="INullableJetStruct.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Interface for Jet structures that are nullable (can have null values).
    /// </summary>
    public interface INullableJetStruct
    {
        /// <summary>
        /// Gets a value indicating whether the structure has a null value.
        /// </summary>
        bool HasValue { get; }
    }
}