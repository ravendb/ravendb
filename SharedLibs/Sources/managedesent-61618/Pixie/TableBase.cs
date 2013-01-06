//-----------------------------------------------------------------------
// <copyright file="TableBase.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Represents a table in a database.
    /// </summary>
    /// <remarks>
    /// This abstract class contains the common code used by 
    /// ReadOnlyTable and ReadWriteTable.
    /// </remarks>
    internal abstract class TableBase : Table
    {
        /// <summary>
        /// A list of Records that have opened their own tableids. These records all have to
        /// be closed when the table is closed.
        /// </summary>
        private readonly List<Record> recordsWithOpenCursor;

        /// <summary>
        /// The name of the table.
        /// </summary>
        private readonly string tablename;

        /// <summary>
        /// The cursor used for this Table object.
        /// </summary>
        private Cursor tableCursor;

        /// <summary>
        /// A cached tableid which OpenCursor and CloseCursor
        /// can use.
        /// </summary>
        private Cursor cachedCursor;

        /// <summary>
        /// Initializes a new instance of the TableBase class.
        /// </summary>
        /// <param name="connection">The connection the table is opened against.</param>
        /// <param name="table">The name of the table to open.</param>
        protected TableBase(ConnectionBase connection, string table)
        {
            this.Tracer = new Tracer("Table", "Esent Table", String.Format("Table {0}", table));
            this.Connection = connection;
            this.tablename = table;

            this.recordsWithOpenCursor = new List<Record>();
            this.tableCursor = this.OpenCursor();
            this.CursorCache = new CursorCache<Cursor>(this.OpenCursor, this.CloseCursor, this.TableName, 4);

            this.LoadMetaData();
            this.Tracer.TraceInfo("opened");
        }

        /// <summary>
        /// Occurs when the table is closed.
        /// </summary>
        public event Action<TableBase> Disposed;

        /// <summary>
        /// Gets a value indicating whether the table is closed.
        /// </summary>
        public bool IsClosed { get; private set; }

        /// <summary>
        /// Gets the connection this table is opened for.
        /// </summary>
        public ConnectionBase Connection { get; private set; }

        /// <summary>
        /// Gets information about columns in the table.
        /// </summary>
        public IDictionary<string, ColumnMetaData> Columns { get; private set; }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName
        {
            get
            {
                this.CheckNotClosed();
                return this.tablename;
            }
        }

        /// <summary>
        /// Gets a tableid cache for the table.
        /// </summary>
        public CursorCache<Cursor> CursorCache { get; private set; }

        /// <summary>
        /// Gets the underlying Session for this table.
        /// </summary>
        public Session Session
        {
            get
            {
                return this.Connection.Session;
            }
        }

        /// <summary>
        /// Gets the underlying JET_DBID for this table.
        /// </summary>
        public JET_DBID Dbid
        {
            get
            {
                return this.Connection.Dbid;
            }
        }

        /// <summary>
        /// Gets the underlying Cursor for this table.
        /// </summary>
        protected Cursor TableCursor
        {
            get
            {
                this.CheckNotClosed();
                return this.tableCursor;
            }
        }

        /// <summary>
        /// Gets or sets the Tracer object for this Table.
        /// </summary>
        protected Tracer Tracer { get; set; }

        /// <summary>
        /// Called when the table is disposed.
        /// </summary>
        public void Dispose()
        {
            this.Close();
            Debug.Assert(this.IsClosed, "Disposed called but isDisposed is false");
        }

        /// <summary>
        /// Create a new record for inserting into the database. Use the Save() method
        /// on the Record to insert the record.
        /// </summary>
        /// <remarks>
        /// Deferred to the ReadOnlyTable and ReadWriteTable classes.
        /// </remarks>
        /// <returns>A new record prepared for insert.</returns>
        public abstract Record NewRecord();

        /// <summary>
        /// Check to see if the table is empty.
        /// </summary>
        /// <returns>True if the are no (visible) record, False otherwise.</returns>
        public bool IsEmpty()
        {
            this.CheckNotClosed();
            return false == this.TableCursor.TryMoveFirst();
        }

        /// <summary>
        /// Returns the first Record in the table. This throws an exception if the table
        /// is empty.
        /// </summary>
        /// <returns>The first record in the table.</returns>
        public Record First()
        {
            this.CheckNotClosed();
            this.TableCursor.MoveFirst();
            return this.CreateRecordFromCurrentPosition(this.TableCursor);
        }

        /// <summary>
        /// Returns the last Record in the table. This throws an exception if the table
        /// is empty.
        /// </summary>
        /// <returns>The last record in the table.</returns>
        public Record Last()
        {
            this.CheckNotClosed();
            this.TableCursor.MoveLast();
            return this.CreateRecordFromCurrentPosition(this.TableCursor);
        }

        /// <summary>
        /// Enumerate all records in the table.
        /// </summary>
        /// <returns>An enumerator over all records in the table.</returns>
        public IEnumerator<Record> GetEnumerator()
        {
            this.CheckNotClosed();
            this.TableCursor.SetSequential();
            try
            {
                this.TableCursor.SetCurrentIndex(string.Empty);
                this.TableCursor.MoveBeforeFirst();
                while (this.TableCursor.TryMoveNext())
                {
                    Record record = this.CreateRecordFromCurrentPosition(this.tableCursor);
                    yield return record;
                    this.CheckNotClosed();
                }
            }
            finally
            {
                this.TableCursor.ResetSequential();
            }
        }

        /// <summary>
        /// Enumerate all records in the table.
        /// </summary>
        /// <returns>An enumerator over all records in the table.</returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// Add a new column to the table.
        /// </summary>
        /// <param name="columndef">The column definition.</param>
        /// <returns>The table the column was added to.</returns>
        public abstract Table CreateColumn(ColumnDefinition columndef);

        /// <summary>
        /// Open a new cursor on the table.
        /// </summary>
        /// <param name="record">The record that is opening the tableid.</param>
        /// <returns>A new JET_TABLEID for the record.</returns>
        /// <remarks>
        /// Records that open a JET_TABLEID will be disposed when the table
        /// is closed.
        /// </remarks>
        public Cursor OpenCursorForRecord(RecordBase record)
        {
            Cursor cursor = this.OpenCursor();
            this.recordsWithOpenCursor.Add(record);
            record.CursorClosedEvent += this.OnRecordClose;

            return cursor;
        }

        /// <summary>
        /// Close a cursor on the table.
        /// </summary>
        /// <param name="cursor">The cursor to close.</param>
        public void CloseCursor(Cursor cursor)
        {
            if (null == this.cachedCursor)
            {
                this.cachedCursor = cursor;
            }
            else
            {
                cursor.Dispose();
            }
        }

        /// <summary>
        /// Make sure the table isn't c;psed.
        /// </summary>
        protected void CheckNotClosed()
        {
            if (this.IsClosed)
            {
                this.Tracer.TraceError("InvalidOperationException: table is closed");
                throw new InvalidOperationException(String.Format("table '{0}' has been closed", this.tablename));
            }
        }

        /// <summary>
        /// Load the meta-data for the table.
        /// </summary>
        protected void LoadMetaData()
        {
            this.LoadColumnMetaData();
        }

        /// <summary>
        /// Create a Record object for the record with the specified bookmark.
        /// </summary>
        /// <remarks>
        /// Implemented by ReadOnlyTable and ReadWriteTable.
        /// </remarks>
        /// <param name="bookmark">The bookmark of the record.</param>
        /// <returns>A new record object.</returns>
        protected abstract Record CreateRecord(Bookmark bookmark);

        /// <summary>
        /// Create a ColumnMetaData object from a ColumnInfo.
        /// </summary>
        /// <param name="columninfo">The ColumnInfo for the column.</param>
        /// <returns>A ColumnMetaData object for the column.</returns>
        private static ColumnMetaData GetMetadata(ColumnInfo columninfo)
        {
            var converter = Dependencies.Container.Resolve<InteropConversion>();
            var dataConversion = Dependencies.Container.Resolve<DataConversion>();

            ColumnMetaData metadata = converter.CreateColumnMetaDataFromColumnInfo(columninfo);

            metadata.ObjectConverter = dataConversion.ConvertToObject[metadata.Type];
            metadata.ObjectToBytesConverter = dataConversion.ConvertObjectToBytes[metadata.Type];
            metadata.BytesToObjectConverter = dataConversion.ConvertBytesToObject[metadata.Type];

            metadata.SetColumn = (cursor, obj) =>
            {
                byte[] data = metadata.ObjectToBytesConverter(metadata.ObjectConverter(obj));
                cursor.SetColumn(metadata.Columnid, data, SetColumnGrbit.None);
            };

            metadata.MakeKey = (cursor, obj, grbit) =>
            {
                byte[] data = metadata.ObjectToBytesConverter(metadata.ObjectConverter(obj));
                cursor.MakeKey(data, grbit);
            };

            metadata.RetrieveColumn = MakeRetrieveColumnFunction(metadata);

            return metadata;
        }

        /// <summary>
        /// Create the column retrieval function for the given metadata.
        /// </summary>
        /// <param name="metadata">The meta-data.</param>
        /// <returns>The meta-data function.</returns>
        private static Func<Cursor, RetrieveColumnGrbit, object> MakeRetrieveColumnFunction(ColumnMetaData metadata)
        {
            switch (metadata.Type)
            {
                case ColumnType.Bool:
                    return (cursor, grbit) => cursor.RetrieveColumnAsBoolean(metadata.Columnid, grbit);
                case ColumnType.Byte:
                    return (cursor, grbit) => cursor.RetrieveColumnAsByte(metadata.Columnid, grbit);
                case ColumnType.Int16:
                    return (cursor, grbit) => cursor.RetrieveColumnAsInt16(metadata.Columnid, grbit);
                case ColumnType.UInt16:
                    return (cursor, grbit) => cursor.RetrieveColumnAsUInt16(metadata.Columnid, grbit);
                case ColumnType.Int32:
                    return (cursor, grbit) => cursor.RetrieveColumnAsInt32(metadata.Columnid, grbit);
                case ColumnType.UInt32:
                    return (cursor, grbit) => cursor.RetrieveColumnAsUInt32(metadata.Columnid, grbit);
                case ColumnType.Int64:
                    return (cursor, grbit) => cursor.RetrieveColumnAsInt64(metadata.Columnid, grbit);
                case ColumnType.Float:
                    return (cursor, grbit) => cursor.RetrieveColumnAsFloat(metadata.Columnid, grbit);
                case ColumnType.Double:
                    return (cursor, grbit) => cursor.RetrieveColumnAsDouble(metadata.Columnid, grbit);
                case ColumnType.DateTime:
                    return (cursor, grbit) => cursor.RetrieveColumnAsDateTime(metadata.Columnid, grbit);
                case ColumnType.Guid:
                    return (cursor, grbit) => cursor.RetrieveColumnAsGuid(metadata.Columnid, grbit);
                case ColumnType.AsciiText:
                    return (cursor, grbit) => cursor.RetrieveColumnAsString(metadata.Columnid, Encoding.ASCII, grbit);
                case ColumnType.Text:
                    return (cursor, grbit) => cursor.RetrieveColumnAsString(metadata.Columnid, Encoding.Unicode, grbit);
                default:
                    return (cursor, grbit) => metadata.BytesToObjectConverter(
                                                                     cursor.RetrieveColumn(metadata.Columnid, grbit));
            }            
        }

        /// <summary>
        /// Load the column meta-data for the table.
        /// </summary>
        private void LoadColumnMetaData()
        {
            this.Columns = new Dictionary<string, ColumnMetaData>(StringComparer.InvariantCultureIgnoreCase);
            using (Transaction trx = this.Connection.BeginTransaction())
            {
                foreach (ColumnInfo columninfo in this.TableCursor.GetColumns())
                {
                    ColumnMetaData metadata = GetMetadata(columninfo);

                    this.Columns[metadata.Name] = metadata;
                }

                trx.Commit();
            }
        }

        /// <summary>
        /// Create a record from the current position of the specified tableid.
        /// </summary>
        /// <param name="cursor">The cursor positioned on the record.</param>
        /// <returns>A new record containing the bookmark of the current position.</returns>
        private Record CreateRecordFromCurrentPosition(Cursor cursor)
        {
            return this.CreateRecord(cursor.GetBookmark());
        }

        /// <summary>
        /// Closes the table, freeing ESE resources.
        /// </summary>
        private void Close()
        {
            if (!this.IsClosed)
            {
                this.DisposeAllRecordsWithOpenTableids();
                this.CursorCache.Close();

                if (null != this.cachedCursor)
                {
                    this.cachedCursor.Dispose();
                }

                if (null != this.tableCursor)
                {
                    this.tableCursor.Dispose();
                }

                this.cachedCursor = null;
                this.tableCursor = null;

                GC.SuppressFinalize(this);
                this.Tracer.TraceInfo("closed");

                if (null != this.Disposed)
                {
                    this.Disposed(this);
                }

                this.IsClosed = true;
            }
        }

        /// <summary>
        /// Open a new cursor for this table.
        /// </summary>
        /// <returns>The new cursor.</returns>
        private Cursor OpenCursor()
        {
            Cursor theCursor;
            if (null != this.cachedCursor)
            {
                theCursor = this.cachedCursor;
                this.cachedCursor = null;
            }
            else
            {
                theCursor = new Cursor(this.Session, this.Dbid, this.TableName);
                this.Tracer.TraceInfo("opening a new Cursor");
            }

            return theCursor;
        }

        /// <summary>
        /// Called when a record that has opened its own tableid is closed.
        /// </summary>
        /// <param name="record">The record being closed.</param>
        private void OnRecordClose(RecordBase record)
        {
            record.CursorClosedEvent -= this.OnRecordClose;
            this.recordsWithOpenCursor.Remove(record);
        }

        /// <summary>
        /// Dispose of all record that currently have their own JET_TABLEID.
        /// </summary>
        private void DisposeAllRecordsWithOpenTableids()
        {
            // Closing the table will modify the list, so make a copy
            Record[] recordsToClose = this.recordsWithOpenCursor.ToArray();
            foreach (Record r in recordsToClose)
            {
                r.Dispose();
            }
        }
    }
}
