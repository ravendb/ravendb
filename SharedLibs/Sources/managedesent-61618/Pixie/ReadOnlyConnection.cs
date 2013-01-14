//-----------------------------------------------------------------------
// <copyright file="ReadOnlyConnection.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A read-only connection to a database.
    /// </summary>
    internal class ReadOnlyConnection : ConnectionBase
    {
        /// <summary>
        /// Initializes a new instance of the ReadOnlyConnection class.
        /// </summary>
        /// <param name="instance">The instance to use for the connection.</param>
        /// <param name="name">The name of the connection.</param>
        /// <param name="database">The database to be connected do.</param>
        public ReadOnlyConnection(Instance instance, string name, string database) :
            base(instance, name + " (RO)", database, OpenDatabaseGrbit.ReadOnly)
        {
        }

        /// <summary>
        /// Gets a value indicating whether the connection is read-only.
        /// </summary>
        public override bool IsReadOnly
        { 
            get
            { 
                return true;
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
            throw new EsentReadOnlyException(this.Name);
        }

        /// <summary>
        /// Create a new table object for the specified table. This is specialized
        /// by the subclasses of Connection.
        /// </summary>
        /// <param name="tablename">The table to create the table object for.</param>
        /// <returns>A new table object.</returns>
        protected override TableBase OpenTableImpl(string tablename)
        {
            return new ReadOnlyTable(this, tablename);
        }
    }
}
