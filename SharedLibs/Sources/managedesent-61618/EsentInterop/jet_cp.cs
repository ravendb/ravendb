//-----------------------------------------------------------------------
// <copyright file="jet_cp.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Codepage for an ESENT column.
    /// </summary>
    public enum JET_CP
    {
        /// <summary>
        /// Code page for non-text columns.
        /// </summary>
        None = 0,

        /// <summary>
        /// Unicode encoding.
        /// </summary>
        Unicode = 1200,

        /// <summary>
        /// ASCII encoding.
        /// </summary>
        ASCII = 1252,
    }
}
