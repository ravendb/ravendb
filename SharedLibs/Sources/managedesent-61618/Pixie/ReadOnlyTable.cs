//-----------------------------------------------------------------------
// <copyright file="ReadOnlyTable.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Represents a read-only table.
    /// </summary>
    internal class ReadOnlyTable : TableBase
    {
        /// <summary>
        /// Initializes a new instance of the ReadOnlyTable class.
        /// </summary>
        /// <param name="connection">The connection the table is opened against.</param>
        /// <param name="table">The name of the table to open.</param>
        public ReadOnlyTable(ConnectionBase connection, string table) : base(connection, table)
        {
            this.Tracer.TraceInfo("read-only");
        }

        /// <summary>
        /// Add a new column to the table. This always fails, as the table is read-only.
        /// </summary>
        /// <param name="columndef">The column definition.</param>
        /// <returns>Always throws an exception.</returns>
        public override Table CreateColumn(ColumnDefinition columndef)
        {
            this.CheckNotClosed();
            throw this.CreateReadOnlyException();
        }

        /// <summary>
        /// Create a new record for inserting into the database. This always fails as the
        /// table is read-only.
        /// </summary>
        /// <returns>Always throws an exception.</returns>
        public override Record NewRecord()
        {
            this.CheckNotClosed();
            throw this.CreateReadOnlyException();
        }

        /// <summary>
        /// Create a Record object for the record with the specified bookmark.
        /// </summary>
        /// <param name="bookmark">The bookmark of the record.</param>
        /// <returns>A new ReadOnlyRecord.</returns>
        protected override Record CreateRecord(Bookmark bookmark)
        {
            return new ReadOnlyRecord(this, bookmark);
        }

        /// <summary>
        /// Creates a new read-only exception for this table.
        /// </summary>
        /// <returns>A read-only exception.</returns>
        private Exception CreateReadOnlyException()
        {
            return new EsentReadOnlyException(this.TableName);
        }
    }
}
