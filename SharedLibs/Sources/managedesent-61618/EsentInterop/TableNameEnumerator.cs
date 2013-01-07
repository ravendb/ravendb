//-----------------------------------------------------------------------
// <copyright file="TableNameEnumerator.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using Microsoft.Isam.Esent.Interop.Implementation;

    /// <summary>
    /// Enumerate the names of tables in a database.
    /// </summary>
    internal sealed class TableNameEnumerator : TableEnumerator<string>
    {
        /// <summary>
        /// The database containing the tables.
        /// </summary>
        private readonly JET_DBID dbid;

        /// <summary>
        /// Object list containing information about tables.
        /// </summary>
        private JET_OBJECTLIST objectlist;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableNameEnumerator"/> class.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        /// <param name="dbid">
        /// The database to get the table names from.
        /// </param>
        public TableNameEnumerator(JET_SESID sesid, JET_DBID dbid) : base(sesid)
        {
            this.dbid = dbid;
        }

        /// <summary>
        /// Open the table to be enumerated. This should set <see cref="TableEnumerator{T}.TableidToEnumerate"/>.
        /// </summary>
        protected override void OpenTable()
        {
            Api.JetGetObjectInfo(this.Sesid, this.dbid, out this.objectlist);
            this.TableidToEnumerate = this.objectlist.tableid;
        }

        /// <summary>
        /// Determine if the current entry in the table being enumerated should
        /// be skipped (not returned). Here we are skipping system tables.
        /// </summary>
        /// <returns>True if the current entry should be skipped.</returns>
        protected override bool SkipCurrent()
        {
            int flags = (int)Api.RetrieveColumnAsInt32(this.Sesid, this.TableidToEnumerate, this.objectlist.columnidflags);
            return ObjectInfoFlags.System == ((ObjectInfoFlags)flags & ObjectInfoFlags.System);
        }

        /// <summary>
        /// Gets the entry the cursor is currently positioned on.
        /// </summary>
        /// <returns>The entry the cursor is currently positioned on.</returns>
        protected override string GetCurrent()
        {
            string name = Api.RetrieveColumnAsString(
                this.Sesid,
                this.TableidToEnumerate,
                this.objectlist.columnidobjectname,
                NativeMethods.Encoding,
                RetrieveColumnGrbit.None);
            return StringCache.TryToIntern(name);
        }
    }
}