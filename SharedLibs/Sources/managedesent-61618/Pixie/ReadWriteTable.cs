//-----------------------------------------------------------------------
// <copyright file="ReadWriteTable.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Represents a read-write table.
    /// </summary>
    internal class ReadWriteTable : TableBase
    {
        /// <summary>
        /// Initializes a new instance of the ReadWriteTable class.
        /// </summary>
        /// <param name="connection">The connection the table is opened against.</param>
        /// <param name="table">The name of the table to open.</param>
        public ReadWriteTable(ConnectionBase connection, string table) : base(connection, table)
        {
            this.Tracer.TraceInfo("read-write");
        }

        /// <summary>
        /// Add a new column to the table.
        /// </summary>
        /// <param name="columndef">The column definition.</param>
        /// <returns>The table the column was added to.</returns>
        public override Table CreateColumn(ColumnDefinition columndef)
        {
            this.Tracer.TraceInfo("adding column {0} of type {1}", columndef.Name, columndef.Type);
            columndef.CreateColumn(this.TableCursor);

            // The meta-data has changed. Reload it.
            this.LoadMetaData();
            return this;
        }

        /// <summary>
        /// Create a new record for inserting into the database. Use the Save() method
        /// on the Record to insert the record.
        /// </summary>
        /// <returns>A new record prepared for insert.</returns>
        public override Record NewRecord()
        {
            this.CheckNotClosed();
            return new ReadWriteRecord(this);
        }

        /// <summary>
        /// Create a Record object for the record with the specified bookmark.
        /// </summary>
        /// <param name="bookmark">The bookmark of the record.</param>
        /// <returns>A new ReadWriteRecord.</returns>
        protected override Record CreateRecord(Bookmark bookmark)
        {
            return new ReadWriteRecord(this, bookmark);
        }
    }
}
