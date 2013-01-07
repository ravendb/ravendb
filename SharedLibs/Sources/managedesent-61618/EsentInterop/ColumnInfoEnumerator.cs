//-----------------------------------------------------------------------
// <copyright file="ColumnInfoEnumerator.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using Microsoft.Isam.Esent.Interop.Implementation;

    /// <summary>
    /// Base class for enumerators that return ColumnInfo objects. Subclasses differ
    /// by how they open the table.
    /// </summary>
    internal abstract class ColumnInfoEnumerator : TableEnumerator<ColumnInfo>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnInfoEnumerator"/> class.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        protected ColumnInfoEnumerator(JET_SESID sesid) : base(sesid)
        {            
        }

        /// <summary>
        /// Gets or sets the columnlist used to retrieve data.
        /// </summary>
        protected JET_COLUMNLIST Columnlist { get; set; }

        /// <summary>
        /// Gets the entry the cursor is currently positioned on.
        /// </summary>
        /// <returns>The entry the cursor is currently positioned on.</returns>
        protected override ColumnInfo GetCurrent()
        {
            return GetColumnInfoFromColumnlist(this.Sesid, this.Columnlist);
        }

        /// <summary>
        /// Create a ColumnInfo object from the data in the current JET_COLUMNLIST entry.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="columnlist">The columnlist to take the data from.</param>
        /// <returns>A ColumnInfo object containing the information from that record.</returns>
        private static ColumnInfo GetColumnInfoFromColumnlist(JET_SESID sesid, JET_COLUMNLIST columnlist)
        {
            string name = Api.RetrieveColumnAsString(
                sesid,
                columnlist.tableid,
                columnlist.columnidcolumnname,
                NativeMethods.Encoding,
                RetrieveColumnGrbit.None);
            name = StringCache.TryToIntern(name);
            var columnidValue = (uint)Api.RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcolumnid);
            var coltypValue = (uint)Api.RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcoltyp);
            uint codepageValue = (ushort)Api.RetrieveColumnAsUInt16(sesid, columnlist.tableid, columnlist.columnidCp);
            var maxLength = (uint)Api.RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcbMax);
            byte[] defaultValue = Api.RetrieveColumn(sesid, columnlist.tableid, columnlist.columnidDefault);
            var grbitValue = (uint)Api.RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidgrbit);

            return new ColumnInfo(
                name,
                new JET_COLUMNID { Value = columnidValue },
                (JET_coltyp)coltypValue,
                (JET_CP)codepageValue,
                checked((int)maxLength),
                defaultValue,
                (ColumndefGrbit)grbitValue);
        }
    }
}