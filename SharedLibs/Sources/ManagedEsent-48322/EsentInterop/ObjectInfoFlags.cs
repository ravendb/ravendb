//-----------------------------------------------------------------------
// <copyright file="ObjectInfoFlags.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Flags for ESENT objects (tables).  Used in <see cref="JET_OBJECTINFO"/>.
    /// </summary>
    [Flags]
    [CLSCompliant(false)]
    public enum ObjectInfoFlags : uint
    {
        /// <summary>
        /// Object is for internal use only.
        /// </summary>
        System = 0x80000000,

        /// <summary>
        /// Table's DDL is fixed.
        /// </summary>
        TableFixedDDL = 0x40000000,

        /// <summary>
        /// Table's DDL is inheritable.
        /// </summary>
        TableTemplate = 0x20000000,

        /// <summary>
        /// Table's DDL is inherited from a template table.
        /// </summary>
        TableDerived = 0x10000000,

        /// <summary>
        /// Fixed or variable columns in derived tables (so that fixed or variable
        /// columns can be added to the template in the future).
        /// Used in conjunction with <see cref="TableTemplate"/>.
        /// </summary>
        TableNoFixedVarColumnsInDerivedTables = 0x04000000,
    }
}
