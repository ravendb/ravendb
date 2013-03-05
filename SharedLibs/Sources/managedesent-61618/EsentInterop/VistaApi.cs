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
        /// Retrieves information about a column in a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database that contains the table.</param>
        /// <param name="tablename">The name of the table containing the column.</param>
        /// <param name="columnid">The ID of the column.</param>
        /// <param name="columnbase">Filled in with information about the columns in the table.</param>
        public static void JetGetColumnInfo(
                JET_SESID sesid,
                JET_DBID dbid,
                string tablename,
                JET_COLUMNID columnid,
                out JET_COLUMNBASE columnbase)
        {
            Api.Check(Api.Impl.JetGetColumnInfo(sesid, dbid, tablename, columnid, out columnbase));
        }

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
        /// Selects a specific instance to be part of the snapshot session.
        /// </summary>
        /// <param name="snapshot">The snapshot identifier.</param>
        /// <param name="instance">The instance to add to the snapshot.</param>
        /// <param name="grbit">Options for this call.</param>
        public static void JetOSSnapshotPrepareInstance(JET_OSSNAPID snapshot, JET_INSTANCE instance, SnapshotPrepareInstanceGrbit grbit)
        {
            Api.Check(Api.Impl.JetOSSnapshotPrepareInstance(snapshot, instance, grbit));
        }

        /// <summary>
        /// Enables log truncation for all instances that are part of the snapshot session.
        /// </summary>
        /// <remarks>
        /// This function should be called only if the snapshot was created with the
        /// <see cref="VistaGrbits.ContinueAfterThaw"/> option. Otherwise, the snapshot
        /// session ends after the call to <see cref="Api.JetOSSnapshotThaw"/>.
        /// </remarks>
        /// <param name="snapshot">The snapshot identifier.</param>
        /// <param name="grbit">Options for this call.</param>
        public static void JetOSSnapshotTruncateLog(JET_OSSNAPID snapshot, SnapshotTruncateLogGrbit grbit)
        {
            Api.Check(Api.Impl.JetOSSnapshotTruncateLog(snapshot, grbit));
        }

        /// <summary>
        /// Truncates the log for a specified instance during a snapshot session.
        /// </summary>
        /// <remarks>
        /// This function should be called only if the snapshot was created with the
        /// <see cref="VistaGrbits.ContinueAfterThaw"/> option. Otherwise, the snapshot
        /// session ends after the call to <see cref="Api.JetOSSnapshotThaw"/>.
        /// </remarks>
        /// <param name="snapshot">The snapshot identifier.</param>
        /// <param name="instance">The instance to truncat the log for.</param>
        /// <param name="grbit">Options for this call.</param>
        public static void JetOSSnapshotTruncateLogInstance(JET_OSSNAPID snapshot, JET_INSTANCE instance, SnapshotTruncateLogGrbit grbit)
        {
            Api.Check(Api.Impl.JetOSSnapshotTruncateLogInstance(snapshot, instance, grbit));
        }

        /// <summary>
        /// Retrieves the list of instances and databases that are part of the
        /// snapshot session at any given moment.
        /// </summary>
        /// <param name="snapshot">The identifier of the snapshot session.</param>
        /// <param name="numInstances">Returns the number of instances.</param>
        /// <param name="instances">Returns information about the instances.</param>
        /// <param name="grbit">Options for this call.</param>
        public static void JetOSSnapshotGetFreezeInfo(
            JET_OSSNAPID snapshot,
            out int numInstances,
            out JET_INSTANCE_INFO[] instances,
            SnapshotGetFreezeInfoGrbit grbit)
        {
            Api.Check(Api.Impl.JetOSSnapshotGetFreezeInfo(snapshot, out numInstances, out instances, grbit));
        }

        /// <summary>
        /// Notifies the engine that the snapshot session finished.
        /// </summary>
        /// <param name="snapshot">The identifier of the snapshot session.</param>
        /// <param name="grbit">Snapshot end options.</param>
        public static void JetOSSnapshotEnd(JET_OSSNAPID snapshot, SnapshotEndGrbit grbit)
        {
            Api.Check(Api.Impl.JetOSSnapshotEnd(snapshot, grbit));
        }

        /// <summary>
        /// Retrieves information about an instance.
        /// </summary>
        /// <param name="instance">The instance to get information about.</param>
        /// <param name="signature">Retrieved information.</param>
        /// <param name="infoLevel">The type of information to retrieve.</param>
        public static void JetGetInstanceMiscInfo(JET_INSTANCE instance, out JET_SIGNATURE signature, JET_InstanceMiscInfo infoLevel)
        {
            Api.Check(Api.Impl.JetGetInstanceMiscInfo(instance, out signature, infoLevel));
        }

        /// <summary>
        /// Initialize the ESENT database engine.
        /// </summary>
        /// <param name="instance">
        /// The instance to initialize. If an instance hasn't been
        /// allocated then a new one is created and the engine
        /// will operate in single-instance mode.
        /// </param>
        /// <param name="recoveryOptions">
        /// Additional recovery parameters for remapping databases during
        /// recovery, position where to stop recovery at, or recovery status.
        /// </param>
        /// <param name="grbit">
        /// Initialization options.
        /// </param>
        /// <returns>
        /// A warning code.
        /// </returns>
        public static JET_wrn JetInit3(ref JET_INSTANCE instance, JET_RSTINFO recoveryOptions, InitGrbit grbit)
        {
            return Api.Check(Api.Impl.JetInit3(ref instance, recoveryOptions, grbit));            
        }

        /// <summary>
        /// Retrieves record size information from the desired location.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">
        /// The cursor that will be used for the API call. The cursor must be
        /// positioned on a record, or have an update prepared.
        /// </param>
        /// <param name="recsize">Returns the size of the record.</param>
        /// <param name="grbit">Call options.</param>
        public static void JetGetRecordSize(JET_SESID sesid, JET_TABLEID tableid, ref JET_RECSIZE recsize, GetRecordSizeGrbit grbit)
        {
            Api.Check(Api.Impl.JetGetRecordSize(sesid, tableid, ref recsize, grbit));
        }
    }
}