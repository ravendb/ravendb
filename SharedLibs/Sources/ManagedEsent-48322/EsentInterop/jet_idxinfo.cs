//-----------------------------------------------------------------------
// <copyright file="jet_idxinfo.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Info levels for retrieving information about indexes.
    /// </summary>
    internal enum JET_IdxInfo
    {
        /// <summary>
        /// Retrieve a <see cref="JET_INDEXLIST"/> containing a list of the indexes.
        /// </summary>
        InfoList = 1,
    }
}
