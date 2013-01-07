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
    public enum ObjectInfoFlags
    {
        /// <summary>
        /// Default options.
        /// </summary>
        None = 0,

        /// <summary>
        /// Object is for internal use only.
        /// </summary>
        System = -2147483648, // 0x80000000
        // It's possible to use bit shift to avoid triggering fxcop CA2217.
        // System = (long)0x1L << 31, // 0x80000000;
        // (http://social.msdn.microsoft.com/Forums/en-US/vstscode/thread/a44aa5c1-c62a-46b7-8009-dc46ba21ba93)
        // But we don't want to change the type of the enum to a long.

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
