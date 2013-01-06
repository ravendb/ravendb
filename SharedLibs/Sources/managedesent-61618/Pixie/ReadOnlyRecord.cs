//-----------------------------------------------------------------------
// <copyright file="ReadOnlyRecord.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Read-only record. This cannot be updated or deleted.
    /// </summary>
    internal class ReadOnlyRecord : RecordBase
    {
        /// <summary>
        /// Initializes a new instance of the ReadOnlyRecord class. This constructor
        /// is used for a record which already exists in the table.
        /// </summary>
        /// <param name="table">The table containing the record.</param>
        /// <param name="bookmark">The bookmark of the record.</param>
        public ReadOnlyRecord(TableBase table, Bookmark bookmark) :
            base(table, bookmark)
        {
        }

        /// <summary>
        /// Save changes in the current update. This always throws an exception.
        /// </summary>
        public override void Save()
        {
            this.CheckNotDisposed();
            throw this.CreateReadOnlyException();
        }

        /// <summary>
        /// Delete the current record. This always throws an exception.
        /// </summary>
        public override void Delete()
        {
            this.CheckNotDisposed();
            throw this.CreateReadOnlyException();
        }

        /// <summary>
        /// Cancel changes in the current update. This always throws an exception.
        /// </summary>
        public override void Cancel()
        {
            this.CheckNotDisposed();
            throw this.CreateReadOnlyException();
        }

        /// <summary>
        /// Set a column in the record.
        /// </summary>
        /// <param name="column">The column to set.</param>
        /// <param name="value">The value to set.</param>
        /// <returns>Always throws an exception.</returns>
        public override Record SetColumn(string column, object value)
        {
            this.CheckNotDisposed();
            throw this.CreateReadOnlyException();
        }

        /// <summary>
        /// Creates a new read-only exception for this table.
        /// </summary>
        /// <returns>A read-only exception.</returns>
        private Exception CreateReadOnlyException()
        {
            return new EsentReadOnlyException(this.ToString());
        }
    }
}