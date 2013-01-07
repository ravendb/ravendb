//-----------------------------------------------------------------------
// <copyright file="ObsoleteApi.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// API members that are marked as obsolete.
    /// </summary>
    public static partial class Api
    {
        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to use.</param>
        /// <param name="tablename">The name of the table to retrieve index information about.</param>
        /// <param name="ignored">This parameter is ignored.</param>
        /// <param name="indexlist">Filled in with information about indexes on the table.</param>
        [Obsolete("Use the overload that takes a JET_IdxInfo parameter, passing in JET_IdxInfo.List")]
        public static void JetGetIndexInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                string ignored,
                out JET_INDEXLIST indexlist)
        {
            Api.JetGetIndexInfo(sesid, dbid, tablename, ignored, out indexlist, JET_IdxInfo.List);
        }

        /// <summary>
        /// Retrieves information about indexes on a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information about.</param>
        /// <param name="indexname">This parameter is ignored.</param>
        /// <param name="indexlist">Filled in with information about indexes on the table.</param>
        [Obsolete("Use the overload that takes a JET_IdxInfo parameter, passing in JET_IdxInfo.List")]
        public static void JetGetTableIndexInfo(
                JET_SESID sesid,
                JET_TABLEID tableid,
                string indexname,
                out JET_INDEXLIST indexlist)
        {
            Api.JetGetTableIndexInfo(sesid, tableid, indexname, out indexlist, JET_IdxInfo.List);
        }
    }
}