//-----------------------------------------------------------------------
// <copyright file="VistaApi.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Vista
{
    /// <summary>
    /// ESENT APIs that were first supported in Windows Vista.
    /// </summary>
    public static class VistaApi
    {
        /// <summary>
        /// Creates a temporary table with a single index. A temporary table
        /// stores and retrieves records just like an ordinary table created
        /// using JetCreateTableColumnIndex. However, temporary tables are
        /// much faster than ordinary tables due to their volatile nature.
        /// They can also be used to very quickly sort and perform duplicate
        /// removal on record sets when accessed in a purely sequential manner.
        /// Also see
        /// <seealso cref="Api.JetOpenTempTable"/>,
        /// <seealso cref="Api.JetOpenTempTable2"/>,
        /// <seealso cref="Api.JetOpenTempTable3"/>.
        /// </summary>
        /// <remarks>
        /// Introduced in Windows Vista. Use <see cref="Api.JetOpenTempTable3"/>
        /// for earlier versions of Esent.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="temporarytable">
        /// Description of the temporary table to create on input. After a
        /// successful call, the structure contains the handle to the temporary
        /// table and column identifications. Use <see cref="Api.JetCloseTable"/>
        /// to free the temporary table when finished.
        /// </param>
        public static void JetOpenTemporaryTable(JET_SESID sesid, JET_OPENTEMPORARYTABLE temporarytable)
        {
            Api.Check(Api.Impl.JetOpenTemporaryTable(sesid, temporarytable));
        }

        /// <summary>
        /// Retrieves performance information from the database engine for the
        /// current thread. Multiple calls can be used to collect statistics
        /// that reflect the activity of the database engine on this thread
        /// between those calls. 
        /// </summary>
        /// <param name="threadstats">Returns the thread statistics data.</param>
        public static void JetGetThreadStats(out JET_THREADSTATS threadstats)
        {
            Api.Check(Api.Impl.JetGetThreadStats(out threadstats));
        }

        /// <summary>
        /// Notifies the engine that the snapshot session finished.
        /// </summary>
        /// <param name="snapid">The identifier of the snapshot session.</param>
        /// <param name="grbit">Snapshot end options.</param>
        public static void JetOSSnapshotEnd(JET_OSSNAPID snapid, SnapshotEndGrbit grbit)
        {
            Api.Check(Api.Impl.JetOSSnapshotEnd(snapid, grbit));
        }
    }
}
