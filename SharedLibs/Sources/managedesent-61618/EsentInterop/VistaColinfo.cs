//-----------------------------------------------------------------------
// <copyright file="VistaColinfo.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Vista
{
    /// <summary>
    /// Column info levels that have been added to the Vista version of ESENT.
    /// </summary>
    public static class VistaColInfo
    {
        /// <summary>
        /// Retrieve the JET_COLBASE using the column id.
        /// </summary>
        internal const JET_ColInfo BaseByColid = (JET_ColInfo)8;

        /// <summary>
        /// For lists, only return non-derived columns (if the table is derived from a template).
        /// </summary>
        internal const JET_ColInfo GrbitNonDerivedColumnsOnly = (JET_ColInfo)int.MinValue; // 0x80000000,

        /// <summary>
        /// For lists, only return the column name and columnid of each column.
        /// </summary>
        internal const JET_ColInfo GrbitMinimalInfo = (JET_ColInfo)0x40000000;

        /// <summary>
        /// For lists, sort returned column list by columnid (default is to sort list by column name).
        /// </summary>
        internal const JET_ColInfo GrbitSortByColumnid = (JET_ColInfo)0x20000000;
    }
}