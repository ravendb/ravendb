//-----------------------------------------------------------------------
// <copyright file="TableColumnInfoEnumerator.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Enumerate columns in a table specified by dbid and name.
    /// </summary>
    internal sealed class TableColumnInfoEnumerator : ColumnInfoEnumerator
    {
        /// <summary>
        /// The database containing the table.
        /// </summary>
        private readonly JET_DBID dbid;

        /// <summary>
        /// The name of the table.
        /// </summary>
        private readonly string tablename;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableColumnInfoEnumerator"/> class.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        /// <param name="dbid">
        /// The database containing the table.
        /// </param>
        /// <param name="tablename">
        /// The name of the table.
        /// </param>
        public TableColumnInfoEnumerator(JET_SESID sesid, JET_DBID dbid, string tablename) : base(sesid)
        {
            this.dbid = dbid;
            this.tablename = tablename;
        }

        /// <summary>
        /// Open the table to be enumerated. This should set <see cref="TableEnumerator{T}.TableidToEnumerate"/>.
        /// </summary>
        protected override void OpenTable()
        {
            JET_COLUMNLIST columnlist;
            Api.JetGetColumnInfo(this.Sesid, this.dbid, this.tablename, string.Empty, out columnlist);
            this.Columnlist = columnlist;
            this.TableidToEnumerate = this.Columnlist.tableid;
        }
    }
}