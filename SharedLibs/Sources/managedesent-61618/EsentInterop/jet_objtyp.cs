//-----------------------------------------------------------------------
// <copyright file="jet_objtyp.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Type of an ESENT object.
    /// </summary>
    public enum JET_objtyp
    {
        /// <summary>
        /// Invalid object type.
        /// </summary>
        Nil = 0,

        /// <summary>
        /// Object is a table.
        /// </summary>
        Table = 1,
    }
}
