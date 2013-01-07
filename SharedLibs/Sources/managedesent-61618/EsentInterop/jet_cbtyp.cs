//-----------------------------------------------------------------------
// <copyright file="jet_cbtyp.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Type of progress being reported.
    /// </summary>
    [Flags]
    public enum JET_cbtyp
    {
        /// <summary>
        /// This callback is reserved and always considered invalid.
        /// </summary>
        Null = 0,

        /// <summary>
        /// A finalizable column has gone to zero.
        /// </summary>
        Finalize = 0x1,

        /// <summary>
        /// This callback will occur just before a new record is inserted into
        /// a table by a call to JetUpdate.
        /// </summary>
        BeforeInsert = 0x2,

        /// <summary>
        /// This callback will occur just after a new record has been inserted
        /// into a table by a call to JetUpdate but before JetUpdate returns.
        /// </summary>
        AfterInsert = 0x4,

        /// <summary>
        /// This callback will occur just prior to an existing record in a table
        /// being changed by a call to JetUpdate.
        /// </summary>
        BeforeReplace = 0x8,

        /// <summary>
        /// This callback will occur just after an existing record in a table
        /// has been changed by a call to JetUpdate but prior to JetUpdate returning.
        /// </summary>
        AfterReplace = 0x10,

        /// <summary>
        /// This callback will occur just before an existing record in a table
        /// is deleted by a call to JetDelete.
        /// </summary>
        BeforeDelete = 0x20,

        /// <summary>
        /// This callback will occur just after an existing record in a table
        /// is deleted by a call to JetDelete.
        /// </summary>
        AfterDelete = 0x40,

        /// <summary>
        /// This callback will occur when the engine needs to retrieve the
        /// user defined default value of a column from the application.
        /// This callback is essentially a limited implementation of
        /// JetRetrieveColumn that is evaluated by the application. A maximum
        /// of one column value can be returned for a user defined default value.
        /// </summary>
        UserDefinedDefaultValue = 0x80,

        /// <summary>
        /// This callback will occur when the online defragmentation of a
        /// database as initiated by JetDefragment has stopped due to either the
        /// process being completed or the time limit being reached.
        /// </summary>
        OnlineDefragCompleted = 0x100,

        /// <summary>
        /// This callback will occur when the application needs to clean up
        /// the context handle for the Local Storage associated with a cursor
        /// that is being released by the database engine. For more information,
        /// see JetSetLS. The delegate for this callback reason is
        /// configured by means of JetSetSystemParameter with JET_paramRuntimeCallback.
        /// </summary>
        FreeCursorLS = 0x200,

        /// <summary>
        /// This callback will occur as the result of the need for the application
        /// to cleanup the context handle for the Local Storage associated with
        /// a table that is being released by the database engine. For more information,
        /// see JetSetLS. The delegate for this callback reason is configured
        /// by means of JetSetSystemParameter with JET_paramRuntimeCallback.
        /// </summary>
        FreeTableLS = 0x400,
    }
}
