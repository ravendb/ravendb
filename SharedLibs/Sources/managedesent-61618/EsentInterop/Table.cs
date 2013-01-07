//-----------------------------------------------------------------------
// <copyright file="Table.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// A class that encapsulates a JET_TABLEID in a disposable object.
    /// This opens an existing table. To create a table use the 
    /// JetCreateTable method.
    /// </summary>
    public class Table : EsentResource
    {
        /// <summary>
        /// The session used to open the table.
        /// </summary>
        private JET_SESID sesid;

        /// <summary>
        /// The underlying JET_TABLEID.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// The name of the table.
        /// </summary>
        private string name;

        /// <summary>
        /// Initializes a new instance of the Table class. The table is
        /// opened from the given database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to open the table in.</param>
        /// <param name="name">The name of the table.</param>
        /// <param name="grbit">JetOpenTable options.</param>
        public Table(JET_SESID sesid, JET_DBID dbid, string name, OpenTableGrbit grbit) 
        {
            this.sesid = sesid;
            this.name = name;
            Api.JetOpenTable(this.sesid, dbid, this.name, null, 0, grbit, out this.tableid);
            this.ResourceWasAllocated();
        }

        /// <summary>
        /// Gets the name of this table.
        /// </summary>
        public string Name
        {
            get
            {
                this.CheckObjectIsNotDisposed();
                return this.name;
            }
        }

        /// <summary>
        /// Gets the JET_TABLEID that this table contains.
        /// </summary>
        public JET_TABLEID JetTableid
        {
            get
            {
                this.CheckObjectIsNotDisposed();
                return this.tableid;
            }
        }

        /// <summary>
        /// Implicit conversion operator from a Table to a JET_TABLEID. This
        /// allows a Table to be used with APIs which expect a JET_TABLEID.
        /// </summary>
        /// <param name="table">The table to convert.</param>
        /// <returns>The JET_TABLEID of the table.</returns>
        public static implicit operator JET_TABLEID(Table table)
        {
            return table.JetTableid;
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="Table"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="Table"/>.
        /// </returns>
        public override string ToString()
        {
            return this.name;
        }

        /// <summary>
        /// Close the table.
        /// </summary>
        public void Close()
        {
            this.CheckObjectIsNotDisposed();
            this.ReleaseResource();
        }

        /// <summary>
        /// Free the underlying JET_TABLEID.
        /// </summary>
        protected override void ReleaseResource()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            this.sesid = JET_SESID.Nil;
            this.tableid = JET_TABLEID.Nil;
            this.name = null;
            this.ResourceWasReleased();
        }
    }
}