//-----------------------------------------------------------------------
// <copyright file="ReadWriteRecord.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Read-write record.
    /// </summary>
    internal class ReadWriteRecord : RecordBase
    {
        /// <summary>
        /// Initializes a new instance of the ReadWriteRecord class. This constructor
        /// is used when a new record is being created and will automatically
        /// prepare an insert.
        /// </summary>
        /// <param name="table">The table containing the record.</param>
        public ReadWriteRecord(TableBase table) : base(table)
        {
        }

        /// <summary>
        /// Initializes a new instance of the ReadWriteRecord class. This constructor
        /// is used for a record which already exists in the table.
        /// </summary>
        /// <param name="table">The table containing the record.</param>
        /// <param name="bookmark">The bookmark of the record.</param>
        public ReadWriteRecord(TableBase table, Bookmark bookmark) :
            base(table, bookmark)
        {
        }

        /// <summary>
        /// Save changes in the current update. This has no effect if no changes have been
        /// made to the record.
        /// </summary>
        public override void Save()
        {
            this.CheckNotDisposed();
            if (this.CurrentlyInUpdate)
            {
                this.Bookmark = this.Cursor.Update();
                this.CurrentlyInUpdate = false;
                this.CloseCursor();
            }

            Debug.Assert(!this.CurrentlyInUpdate, "Still in update after call to Save()");
        }

        /// <summary>
        /// Delete the current record. This disposes the current object.
        /// </summary>
        public override void Delete()
        {
            this.CheckNotDisposed();
            if (null == this.Bookmark)
            {
                throw new EsentException("Cannot delete a new record.");
            }

            this.Cancel();
            this.Cursor.Delete();
            this.Dispose();
        }

        /// <summary>
        /// Cancel changes in the current update. This has no effect if no changes have been
        /// made to the record.
        /// </summary>
        public override void Cancel()
        {
            this.CheckNotDisposed();
            if (this.CurrentlyInUpdate)
            {
                this.Cursor.CancelUpdate();
                this.CurrentlyInUpdate = false;
                this.CloseCursor();

                if (null == this.Bookmark)
                {
                    // this record was going to be inserted into the table. with the update
                    // cancelled the record is now useless
                    this.Dispose();
                }
            }

            Debug.Assert(!this.CurrentlyInUpdate, "Still in update after call to Save()");
        }

        /// <summary>
        /// Set a column in the record.
        /// </summary>
        /// <param name="column">The column to set.</param>
        /// <param name="value">The value to set.</param>
        /// <returns>The record the column was set on.</returns>
        public override Record SetColumn(string column, object value)
        {
            if (!this.CurrentlyInUpdate)
            {
                this.PrepareReplace();
            }

            Debug.Assert(this.HasOwnCursor, "Expected this record to have its own Cursor");
            try
            {
                this.Table.Columns[column].SetColumn(this.Cursor, value);
                return this;
            }
            catch (InvalidCastException ex)
            {
                throw new EsentInvalidConversionException(this.Table.TableName, column, this.Table.Columns[column].Type, value.GetType(), ex);
            }
            catch (KeyNotFoundException ex)
            {
                throw new EsentColumnNotFoundException(this.Table.TableName, column, ex);
            }
        }
    }
}