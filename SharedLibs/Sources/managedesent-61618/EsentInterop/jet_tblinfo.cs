//-----------------------------------------------------------------------
// <copyright file="jet_tblinfo.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Info levels for retrieving table info with JetGetTableInfo.
    /// </summary>
    public enum JET_TblInfo
    {
        /// <summary>
        /// Default option. Retrieves a <see cref="JET_OBJECTINFO"/> containing
        /// information about the table. Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,out Microsoft.Isam.Esent.Interop.JET_OBJECTINFO,Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        Default = 0,

        /// <summary>
        /// Retrieves the name of the table. Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,out string,Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        Name = 1,

        /// <summary>
        /// Retrieves the <see cref="JET_DBID"/> of the database containing the
        /// table. Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,out Microsoft.Isam.Esent.Interop.JET_DBID,Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        Dbid = 2,

        /// <summary>
        /// The behavior of the method depends on how large the array that is passed
        /// to the method is. The array must have at least two entries. 
        /// The first entry will contain the number of Owned Extents in the table.
        /// The second entry will contain the number of Available Extents in the table.
        /// If the array has more than two entries then the remaining bytes of
        /// the buffer will consist of an array of structures that represent a list of
        /// extents. This structure contains two members: the last page number in the
        /// extent and the number of pages in the extent. Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,int[],Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        SpaceUsage = 7,

        /// <summary>
        /// The array passed to JetGetTableInfo must have two entries. 
        /// The first entry will be set to the number of pages in the table.
        /// The second entry will be set to the target density of pages for the table.
        /// Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,int[],Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        SpaceAlloc = 9,

        /// <summary>
        /// Gets the number of owned pages in the table. Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,out int,Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        SpaceOwned = 10,

        /// <summary>
        /// Gets the number of available pages in the table. Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,out int,Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        SpaceAvailable = 11,

        /// <summary>
        /// If the table is a derived table, the result will be filled in with the
        /// name of the table from which the derived table inherited its DDL. If
        /// the table is not a derived table, the buffer will an empty string.
        /// Use this option with
        /// <see cref="Api.JetGetTableInfo(Microsoft.Isam.Esent.Interop.JET_SESID,Microsoft.Isam.Esent.Interop.JET_TABLEID,out string,Microsoft.Isam.Esent.Interop.JET_TblInfo)"/>.
        /// </summary>
        TemplateTableName = 12,
    }
}