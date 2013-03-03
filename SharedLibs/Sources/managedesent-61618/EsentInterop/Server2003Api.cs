//-----------------------------------------------------------------------
// <copyright file="Server2003Api.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Server2003
{
    /// <summary>
    /// APIs that have been added to the Windows Server 2003 version of ESENT.
    /// </summary>
    public static class Server2003Api
    {
        /// <summary>
        /// Notifies the engine that it can resume normal IO operations after a
        /// freeze period ended with a failed snapshot.
        /// </summary>
        /// <param name="snapid">Identifier of the snapshot session.</param>
        /// <param name="grbit">Options for this call.</param>
        public static void JetOSSnapshotAbort(JET_OSSNAPID snapid, SnapshotAbortGrbit grbit)
        {
            Api.Check(Api.Impl.JetOSSnapshotAbort(snapid, grbit));
        }

        /// <summary>
        /// The JetUpdate function performs an update operation including inserting a new row into
        /// a table or updating an existing row. Deleting a table row is performed by calling
        /// <see cref="Api.JetDelete"/>.
        /// </summary>
        /// <param name="sesid">The session which started the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <param name="bookmark">Returns the bookmark of the updated record. This can be null.</param>
        /// <param name="bookmarkSize">The size of the bookmark buffer.</param>
        /// <param name="actualBookmarkSize">Returns the actual size of the bookmark.</param>
        /// <param name="grbit">Update options.</param>
        /// <remarks>
        /// JetUpdate is the final step in performing an insert or an update. The update is begun by
        /// calling <see cref="Api.JetPrepareUpdate"/> and then by calling
        /// <see cref="Api.JetSetColumn(JET_SESID,JET_TABLEID,JET_COLUMNID,byte[],int,SetColumnGrbit,JET_SETINFO)"/>
        /// one or more times to set the record state. Finally, <see cref="JetUpdate2"/>
        /// is called to complete the update operation. Indexes are updated only by JetUpdate or and not during JetSetColumn.
        /// </remarks>
        public static void JetUpdate2(JET_SESID sesid, JET_TABLEID tableid, byte[] bookmark, int bookmarkSize, out int actualBookmarkSize, UpdateGrbit grbit)
        {
            Api.Check(Api.Impl.JetUpdate2(sesid, tableid, bookmark, bookmarkSize, out actualBookmarkSize, grbit));
        }
    }
}