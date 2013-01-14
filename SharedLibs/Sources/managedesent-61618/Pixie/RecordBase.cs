//-----------------------------------------------------------------------
// <copyright file="RecordBase.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// One record in a table. A record is identified by a 
    /// table and a bookmark.
    /// </summary>
    /// <remarks>
    /// A record can be 'lightweight' or 'heavyweight'. A
    /// lightweight record uses the JET_TABLEID in the Table object
    /// that opened the record. A heavyweight record has its own
    /// JET_TABLEID. Heavyweight records are more expensive and have
    /// to be closed before the Table can be closed. Heavyweight
    /// records are required for updates as updates are per-JET_TABLEID.
    /// </remarks>
    internal abstract class RecordBase : Record
    {
        /// <summary>
        /// The table containing this Record.
        /// </summary>
        private readonly TableBase table;

        /// <summary>
        /// The ID of the record object. This ID has nothing to do with
        /// logical record location, it just identifies a specific Record
        /// object. Record object that reference the same database object
        /// can have different ids.
        /// </summary>
        private readonly long id;

        /// <summary>
        /// The last record ID allocated.
        /// </summary>
        private static long lastId = 1;

        /// <summary>
        /// Tracing object 
        /// </summary>
        private static Tracer tracer = new Tracer("Record", "Esent Record object", "Record");

        /// <summary>
        /// Initializes a new instance of the RecordBase class. This constructor
        /// is used when a new record is being created and will automatically
        /// prepare an insert.
        /// </summary>
        /// <param name="table">The table containing the record.</param>
        protected RecordBase(TableBase table) : this()
        {
            this.table = table;
            this.PrepareInsert();
        }

        /// <summary>
        /// Initializes a new instance of the RecordBase class. This constructor
        /// is used for a record which already exists in the table.
        /// </summary>
        /// <param name="table">The table containing the record.</param>
        /// <param name="bookmark">The bookmark of the record.</param>
        protected RecordBase(TableBase table, Bookmark bookmark) :
            this()
        {
            this.table = table;
            this.Bookmark = bookmark;
        }

        /// <summary>
        /// Initializes a new instance of the RecordBase class from being created.
        /// </summary>
        protected RecordBase()
        {
            this.id = Interlocked.Increment(ref RecordBase.lastId);
        }

        /// <summary>
        /// This event is raised when this record closes its cursor.
        /// </summary>
        public event Action<RecordBase> CursorClosedEvent;

        /// <summary>
        /// Gets the table that this record is for.
        /// </summary>
        public virtual TableBase Table
        {
            get
            {
                this.CheckNotDisposed();
                return this.table;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the Record has allocated its own cursor.
        /// </summary>
        protected bool HasOwnCursor { get; set; }

        /// <summary>
        /// Gets or sets the cursor on the table. 
        /// </summary>
        protected Cursor MyCursor { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the record is in an update.
        /// </summary>
        protected bool CurrentlyInUpdate { get; set; }

        /// <summary>
        /// Gets a value indicating whether the record is disposed.
        /// </summary>
        protected bool IsDisposed { get; private set; }

        /// <summary>
        /// Gets or sets the bookmark for the record. This will be null
        /// for records that are being inserted (a record doesn't
        /// get a bookmark until it is inserted into the table).
        /// </summary>
        protected Bookmark? Bookmark { get; set; }

        /// <summary>
        /// Gets the Session for this record.
        /// </summary>
        protected Session Session
        {
            get
            {
                return this.Table.Connection.Session;
            }
        }

        /// <summary>
        /// Gets the public ID of this record.
        /// </summary>
        protected long Id
        { 
            get 
            { 
                return this.id; 
            } 
        }

        /// <summary>
        /// Gets the Cursor for this record.
        /// </summary>
        protected Cursor Cursor
        {
            get
            {
                Cursor cursor;
                if (this.HasOwnCursor)
                {
                    // Our Cursor is always on the record we want
                    return this.MyCursor;
                }
                else if (this.Table.CursorCache.TryGetCursor(this.Id, out cursor))
                {
                    // Our cached cursor will be where we left it.
                    return cursor;
                }
                else
                {
                    // A different record moved our cursor. Reposition it.
                    cursor = this.Table.CursorCache.GetNewCursor(this.Id);
                    cursor.GotoBookmark(this.Bookmark.Value);
                    return cursor;
                }
            }
        }

        /// <summary>
        /// Gets or sets the columns in the record.
        /// </summary>
        /// <param name="column">Name of the column.</param>
        /// <returns>The column as an object.</returns>
        public object this[string column]
        {
            get
            {
                this.CheckNotDisposed();
                return this.RetrieveColumn(column);
            }

            set
            {
                this.CheckNotDisposed();
                this.SetColumn(column, value);
            }
        }

        /// <summary>
        /// Dispose of the object.
        /// </summary>
        public virtual void Dispose()
        {
            this.Cancel();
            this.CloseCursor();
            this.IsDisposed = true;
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Save changes in the current update. This has no effect if no changes have been
        /// made to the record.
        /// </summary>
        /// <remarks>
        /// Implemented by ReadOnlyRecord and ReadWriteRecord.
        /// </remarks>
        public abstract void Save();

        /// <summary>
        /// Delete the current record. This disposes the current object.
        /// </summary>
        /// <remarks>
        /// Implemented by ReadOnlyRecord and ReadWriteRecord.
        /// </remarks>
        public abstract void Delete();

        /// <summary>
        /// Cancel changes in the current update. This has no effect if no changes have been
        /// made to the record.
        /// </summary>
        /// <remarks>
        /// Implemented by ReadOnlyRecord and ReadWriteRecord.
        /// </remarks>
        public abstract void Cancel();

        /// <summary>
        /// Set a column in the record.
        /// </summary>
        /// <remarks>
        /// Implemented by ReadOnlyRecord and ReadWriteRecord.
        /// </remarks>
        /// <param name="column">The column to set.</param>
        /// <param name="value">The value to set.</param>
        /// <returns>The record the column was set on.</returns>
        public abstract Record SetColumn(string column, object value);

        /// <summary>
        /// Start an insert for the current record. This opens a new JET_TABLEID.
        /// </summary>
        protected void PrepareInsert()
        {
            Debug.Assert(!this.HasOwnCursor, "Record should not have its own tableid");
            Debug.Assert(!this.CurrentlyInUpdate, "Record is already in an update");
            this.CheckConnectionIsInTransaction();
            this.OpenOwnCursor();
            this.Cursor.PrepareUpdate(JET_prep.Insert);
            this.CurrentlyInUpdate = true;
        }

        /// <summary>
        /// Start a replace for the current record. This opens a new JET_TABLEID if
        /// the record doesn't already have one.
        /// </summary>
        protected void PrepareReplace()
        {
            Debug.Assert(!this.HasOwnCursor, "Record should not have its own tableid");
            Debug.Assert(!this.CurrentlyInUpdate, "Record is already in an update");
            this.CheckConnectionIsInTransaction();
            this.OpenOwnCursor();
            this.Cursor.GotoBookmark(this.Bookmark.Value);
            this.Cursor.PrepareUpdate(JET_prep.Replace);
            this.CurrentlyInUpdate = true;
        }

        /// <summary>
        /// Open a new JET_TABLEID, if this record doesn't already have one.
        /// </summary>
        protected void OpenOwnCursor()
        {
            Debug.Assert(!this.HasOwnCursor, "Record already has its own tableid");
            Debug.Assert(null == this.MyCursor, "Record has a cursor");
            this.MyCursor = this.Table.OpenCursorForRecord(this);
            this.HasOwnCursor = true;
            Debug.Assert(null != this.MyCursor, "Record does not have a tableid");
        }

        /// <summary>
        /// Close the Cursor allocated by OpenJetTableid.
        /// </summary>
        protected void CloseCursor()
        {
            Debug.Assert(!this.CurrentlyInUpdate, "Record is in an update");
            if (this.HasOwnCursor)
            {
                if (null != this.CursorClosedEvent)
                {
                    this.CursorClosedEvent(this);
                }

                this.table.CloseCursor(this.MyCursor);
                this.HasOwnCursor = false;
                this.MyCursor = null;
            }

            Debug.Assert(!this.HasOwnCursor, "Record still has own tableid");
            Debug.Assert(null == this.MyCursor, "Record has a tableid");
        }

        /// <summary>
        /// Check that the Connection this record is for is in a transaction.
        /// This should be used when preparing an update.
        /// </summary>
        protected void CheckConnectionIsInTransaction()
        {
            if (!this.Table.Connection.InTransaction)
            {
                throw new EsentException("Must be in a transaction to modify a Record");
            }
        }

        /// <summary>
        /// Throw an exception if the record has been disposed.
        /// </summary>
        protected void CheckNotDisposed()
        {
            if (this.IsDisposed)
            {
                throw new ObjectDisposedException("Record");
            }
        }

        /// <summary>
        /// Retrieve a column from the record.
        /// </summary>
        /// <param name="column">The column to retrieve.</param>
        /// <returns>The value of the column.</returns>
        private object RetrieveColumn(string column)
        {
            RetrieveColumnGrbit grbit = this.CurrentlyInUpdate ? RetrieveColumnGrbit.RetrieveCopy : RetrieveColumnGrbit.None;

            try
            {
                return this.Table.Columns[column].RetrieveColumn(this.Cursor, grbit);
            }
            catch (KeyNotFoundException ex)
            {
                throw new EsentColumnNotFoundException(this.Table.TableName, column, ex);
            }
        }
    }
}