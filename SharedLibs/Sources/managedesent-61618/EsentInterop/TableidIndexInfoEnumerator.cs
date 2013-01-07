//-----------------------------------------------------------------------
// <copyright file="TableidIndexInfoEnumerator.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Enumerate columns in a table specified by a tableid.
    /// </summary>
    internal sealed class TableidIndexInfoEnumerator : IndexInfoEnumerator
    {
        /// <summary>
        /// The table to get the column information from.
        /// </summary>
        private readonly JET_TABLEID tableid;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableidIndexInfoEnumerator"/> class.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        /// <param name="tableid">
        /// The table to get column information from.
        /// </param>
        public TableidIndexInfoEnumerator(JET_SESID sesid, JET_TABLEID tableid)
            : base(sesid)
        {
            this.tableid = tableid;
        }

        /// <summary>
        /// Open the table to be enumerated. This should set <see cref="TableEnumerator{T}.TableidToEnumerate"/>.
        /// </summary>
        protected override void OpenTable()
        {
            JET_INDEXLIST indexlist;
            Api.JetGetTableIndexInfo(this.Sesid, this.tableid, string.Empty, out indexlist, JET_IdxInfo.List);
            this.Indexlist = indexlist;
            this.TableidToEnumerate = this.Indexlist.tableid;
        }
    }
}