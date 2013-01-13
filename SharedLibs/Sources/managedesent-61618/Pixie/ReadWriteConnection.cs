//-----------------------------------------------------------------------
// <copyright file="ReadWriteConnection.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A read-write connection to a database.
    /// </summary>
    internal class ReadWriteConnection : ConnectionBase
    {
        /// <summary>
        /// Initializes a new instance of the ReadWriteConnection class.
        /// </summary>
        /// <param name="instance">The instance to use for the connection.</param>
        /// <param name="name">The name of the connection.</param>
        /// <param name="database">The database to be connected do.</param>
        public ReadWriteConnection(Instance instance, string name, string database) :
            base(instance, name + " (RW)", database, OpenDatabaseGrbit.None)
        {
        }

        /// <summary>
        /// Gets a value indicating whether the connection is read-only.
        /// </summary>
        public override bool IsReadOnly
        { 
            get
            { 
                return false;
            }
        }

        /// <summary>
        /// Create the given table.
        /// </summary>
        /// <param name="tablename">The table to create.</param>
        /// <returns>A new Table object for the table.</returns>
        public override Table CreateTable(string tablename)
        {
            this.CheckNotDisposed();

            JET_TABLEID tableid;
            Api.JetCreateTable(this.Session, this.Dbid, tablename, 16, 100, out tableid);
            Api.JetCloseTable(this.Session, tableid);
            this.Tracer.TraceInfo("created table '{0}'", tablename);
            return this.OpenTable(tablename);
        }

        /// <summary>
        /// Create a new table object for the specified table. This is specialized
        /// by the subclasses of Connection.
        /// </summary>
        /// <param name="tablename">The table to create the table object for.</param>
        /// <returns>A new table object.</returns>
        protected override TableBase OpenTableImpl(string tablename)
        {
            return new ReadWriteTable(this, tablename);
        }
    }
}
