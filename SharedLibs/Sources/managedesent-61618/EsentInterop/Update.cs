//-----------------------------------------------------------------------
// <copyright file="Update.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Globalization;

    /// <summary>
    /// A class that encapsulates an update on a JET_TABLEID.
    /// </summary>
    public class Update : EsentResource
    {
        /// <summary>
        /// The underlying JET_SESID.
        /// </summary>
        private readonly JET_SESID sesid;

        /// <summary>
        /// The underlying JET_TABLEID.
        /// </summary>
        private readonly JET_TABLEID tableid;

        /// <summary>
        /// The type of update.
        /// </summary>
        private readonly JET_prep prep;

        /// <summary>
        /// Initializes a new instance of the Update class. This automatically
        /// begins an update. The update will be cancelled if
        /// not explicitly saved.
        /// </summary>
        /// <param name="sesid">The session to start the transaction for.</param>
        /// <param name="tableid">The tableid to prepare the update for.</param>
        /// <param name="prep">The type of update.</param>
        public Update(JET_SESID sesid, JET_TABLEID tableid, JET_prep prep)
        {
            if (JET_prep.Cancel == prep)
            {
                throw new ArgumentException("Cannot create an Update for JET_prep.Cancel", "prep");
            }

            this.sesid = sesid;
            this.tableid = tableid;
            this.prep = prep;
            Api.JetPrepareUpdate(this.sesid, this.tableid, this.prep);
            this.ResourceWasAllocated();
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="Update"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="Update"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "Update ({0})", this.prep);
        }

        /// <summary>
        /// Update the tableid.
        /// </summary>
        /// <param name="bookmark">Returns the bookmark of the updated record. This can be null.</param>
        /// <param name="bookmarkSize">The size of the bookmark buffer.</param>
        /// <param name="actualBookmarkSize">Returns the actual size of the bookmark.</param>
        /// <remarks>
        /// Save is the final step in performing an insert or an update. The update is begun by
        /// calling creating an Update object and then by calling JetSetColumn or JetSetColumns one or more times
        /// to set the record state. Finally, Update is called to complete the update operation.
        /// Indexes are updated only by Update or and not during JetSetColumn or JetSetColumns
        /// </remarks>
        public void Save(byte[] bookmark, int bookmarkSize, out int actualBookmarkSize)
        {
            this.CheckObjectIsNotDisposed();
            if (!this.HasResource)
            {
                throw new InvalidOperationException("Not in an update");
            }

            Api.JetUpdate(this.sesid, this.tableid, bookmark, bookmarkSize, out actualBookmarkSize);
            this.ResourceWasReleased();
        }

        /// <summary>
        /// Update the tableid.
        /// </summary>
        /// <remarks>
        /// Save is the final step in performing an insert or an update. The update is begun by
        /// calling creating an Update object and then by calling JetSetColumn or JetSetColumns one or more times
        /// to set the record state. Finally, Update is called to complete the update operation.
        /// Indexes are updated only by Update or and not during JetSetColumn or JetSetColumns
        /// </remarks>
        public void Save()
        {
            int ignored;
            this.Save(null, 0, out ignored);
        }

        /// <summary>
        /// Update the tableid and position the tableid on the record that was modified.
        /// This can be useful when inserting a record because by default the tableid
        /// remains in its old location.
        /// </summary>
        /// <remarks>
        /// Save is the final step in performing an insert or an update. The update is begun by
        /// calling creating an Update object and then by calling JetSetColumn or JetSetColumns one or more times
        /// to set the record state. Finally, Update is called to complete the update operation.
        /// Indexes are updated only by Update or and not during JetSetColumn or JetSetColumns
        /// </remarks>
        public void SaveAndGotoBookmark()
        {
            var bookmark = Caches.BookmarkCache.Allocate();
            int actualBookmarkSize;
            this.Save(bookmark, bookmark.Length, out actualBookmarkSize);
            Api.JetGotoBookmark(this.sesid, this.tableid, bookmark, actualBookmarkSize);
            Caches.BookmarkCache.Free(ref bookmark);
        }

        /// <summary>
        /// Cancel the update.
        /// </summary>
        public void Cancel()
        {
            this.CheckObjectIsNotDisposed();
            if (!this.HasResource)
            {
                throw new InvalidOperationException("Not in an update");
            }

            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Cancel);
            this.ResourceWasReleased();
        }

        /// <summary>
        /// Called when the transaction is being disposed while active.
        /// This should rollback the transaction.
        /// </summary>
        protected override void ReleaseResource()
        {
            this.Cancel();
        }
    }
}