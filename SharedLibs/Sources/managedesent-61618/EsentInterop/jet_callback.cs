//-----------------------------------------------------------------------
// <copyright file="jet_callback.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// A multi-purpose callback function used by the database engine to inform
    /// the application of an event involving online defragmentation and cursor
    /// state notifications. 
    /// </summary>
    /// <param name="sesid">The session for which the callback is being made.</param>
    /// <param name="dbid">The database for which the callback is being made.</param>
    /// <param name="tableid">The cursor for which the callback is being made.</param>
    /// <param name="cbtyp">The operation for which the callback is being made.</param>
    /// <param name="arg1">First callback-specific argument.</param>
    /// <param name="arg2">Second callback-specific argument.</param>
    /// <param name="context">Callback context.</param>
    /// <param name="unused">This parameter is not used.</param>
    /// <returns>An ESENT error code.</returns>
    public delegate JET_err JET_CALLBACK(
        JET_SESID sesid,
        JET_DBID dbid,
        JET_TABLEID tableid,
        JET_cbtyp cbtyp,
        object arg1,
        object arg2,
        IntPtr context,
        IntPtr unused);
}
