//-----------------------------------------------------------------------
// <copyright file="jet_colinfo.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Info levels for retrieving column info.
    /// </summary>
    internal enum JET_ColInfo
    {
        /// <summary>
        /// Default option. Retrieves a JET_COLUMNDEF.
        /// </summary>
        Default = 0,

        /// <summary>
        /// Retrieves a JET_COLUMNLIST structure, containing all the columns
        /// in the table.
        /// </summary>
        List = 1,

        /// <summary>
        /// Retrieves a JET_COLUMNBASE structure.
        /// </summary>
        Base = 4,

        /// <summary>
        /// Retrieves a JET_COLUMNDEF, the szColumnName argument is interpreted
        /// as a pointer to a columnid.
        /// </summary>
        ByColid = 6,
    }
}
