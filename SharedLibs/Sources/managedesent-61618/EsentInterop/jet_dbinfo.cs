//-----------------------------------------------------------------------
// <copyright file="jet_dbinfo.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Info levels for retrieving database info.
    /// </summary>
    public enum JET_DbInfo
    {
        /// <summary>
        /// Returns the path to the database file (string).
        /// </summary>
        Filename = 0,

        /// <summary>
        /// Returns the locale identifier (LCID) associated with this database (Int32).
        /// </summary>
        LCID = 3,

        /// <summary>
        /// Returns a <see cref="OpenDatabaseGrbit"/>. This indicates whether the
        /// database is opened in exclusive mode. If the database is in exclusive mode then 
        /// <see cref="OpenDatabaseGrbit.Exclusive"/> will be returned, otherwise zero is
        /// returned. Other database grbit options for JetAttachDatabase and JetOpenDatabase
        /// are not returned.
        /// </summary>
        Options = 6,

        /// <summary>
        /// Returns a number one greater than the maximum level to which transactions can be
        /// nested. If <see cref="Api.JetBeginTransaction"/> is called (in a nesting fashion, that is, on the
        /// same session, without a commit or rollback) as many times as this value, on the
        /// last call <see cref="JET_err.TransTooDeep"/> will be returned (Int32).
        /// </summary>
        Transactions = 7,

        /// <summary>
        /// Returns the major version of the database engine (Int32).
        /// </summary>
        Version = 8,

        /// <summary>
        /// Returns the filesize of the database, in pages (Int32).
        /// </summary>
        Filesize = 10,

        /// <summary>
        /// Returns the owned space of the database, in pages (Int32).
        /// </summary>
        SpaceOwned = 11,

        /// <summary>
        /// Returns the available space in the database, in pages (Int32).
        /// </summary>
        SpaceAvailable = 12,

        /// <summary>
        /// Returns a <see cref="JET_DBINFOMISC"/> object.
        /// </summary>
        Misc = 14,

        /// <summary>
        /// Returns a boolean indicating whether the database is attached (boolean).
        /// </summary>
        DBInUse = 15,

        /// <summary>
        /// Returns the page size of the database (Int32).
        /// </summary>
        PageSize = 17,

        /// <summary>
        /// Returns the type of the database (<see cref="JET_filetype"/>).
        /// </summary>
        FileType = 19,
    }
}