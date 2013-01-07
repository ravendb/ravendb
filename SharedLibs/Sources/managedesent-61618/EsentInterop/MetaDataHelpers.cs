//-----------------------------------------------------------------------
// <copyright file="MetaDataHelpers.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Microsoft.Isam.Esent.Interop.Implementation;

    /// <summary>
    /// Helper methods for the ESENT API. These methods deal with database
    /// meta-data.
    /// </summary>
    public static partial class Api
    {
        /// <summary>
        /// Try to open a table.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database to look for the table in.</param>
        /// <param name="tablename">The name of the table.</param>
        /// <param name="grbit">Table open options.</param>
        /// <param name="tableid">Returns the opened tableid.</param>
        /// <returns>True if the table was opened, false if the table doesn't exist.</returns>
        public static bool TryOpenTable(
            JET_SESID sesid,
            JET_DBID dbid,
            string tablename,
            OpenTableGrbit grbit,
            out JET_TABLEID tableid)
        {
            var err = (JET_err)Impl.JetOpenTable(sesid, dbid, tablename, null, 0, grbit, out tableid);
            if (JET_err.ObjectNotFound == err)
            {
                return false;
            }

            Api.Check((int)err);
            Debug.Assert(err >= JET_err.Success, "Exception should have been thrown in case of error");
            return true;
        }

        /// <summary>
        /// Creates a dictionary which maps column names to their column IDs.
        /// </summary>
        /// <param name="sesid">The sesid to use.</param>
        /// <param name="tableid">The table to retrieve the information for.</param>
        /// <returns>A dictionary mapping column names to column IDs.</returns>
        public static IDictionary<string, JET_COLUMNID> GetColumnDictionary(JET_SESID sesid, JET_TABLEID tableid)
        {
            JET_COLUMNLIST columnlist;
            JetGetTableColumnInfo(sesid, tableid, string.Empty, out columnlist);
            try
            {
                // esent treats column names as case-insensitive, so we want the dictionary to be case insensitive as well
                var dict = new Dictionary<string, JET_COLUMNID>(
                    columnlist.cRecord, StringComparer.OrdinalIgnoreCase);
                if (columnlist.cRecord > 0)
                {
                    if (Api.TryMoveFirst(sesid, columnlist.tableid))
                    {
                        do
                        {
                            string name = RetrieveColumnAsString(
                                sesid,
                                columnlist.tableid,
                                columnlist.columnidcolumnname,
                                NativeMethods.Encoding,
                                RetrieveColumnGrbit.None);
                            name = StringCache.TryToIntern(name);
                            var columnidValue =
                                (uint)RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcolumnid);

                            var columnid = new JET_COLUMNID { Value = columnidValue };
                            dict.Add(name, columnid);
                        }
                        while (TryMoveNext(sesid, columnlist.tableid));
                    }
                }

                return dict;
            }
            finally
            {
                // Close the temporary table used to return the results
                JetCloseTable(sesid, columnlist.tableid);
            }
        }

        /// <summary>
        /// Get the columnid of the specified column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table containing the column.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The id of the column.</returns>
        public static JET_COLUMNID GetTableColumnid(JET_SESID sesid, JET_TABLEID tableid, string columnName)
        {
            JET_COLUMNDEF columndef;
            JetGetTableColumnInfo(sesid, tableid, columnName, out columndef);
            return columndef.columnid;
        }

        /// <summary>
        /// Iterates over all the columns in the table, returning information about each one.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve column information for.</param>
        /// <returns>An iterator over ColumnInfo for each column in the table.</returns>
        public static IEnumerable<ColumnInfo> GetTableColumns(JET_SESID sesid, JET_TABLEID tableid)
        {
            return new GenericEnumerable<ColumnInfo>(() => new TableidColumnInfoEnumerator(sesid, tableid));
        }

        /// <summary>
        /// Iterates over all the columns in the table, returning information about each one.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database containing the table.</param>
        /// <param name="tablename">The name of the table.</param>
        /// <returns>An iterator over ColumnInfo for each column in the table.</returns>
        public static IEnumerable<ColumnInfo> GetTableColumns(JET_SESID sesid, JET_DBID dbid, string tablename)
        {
            if (null == tablename)
            {
                throw new ArgumentNullException("tablename");    
            }

            return new GenericEnumerable<ColumnInfo>(() => new TableColumnInfoEnumerator(sesid, dbid, tablename));
        }

        /// <summary>
        /// Iterates over all the indexes in the table, returning information about each one.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information for.</param>
        /// <returns>An iterator over an IndexInfo for each index in the table.</returns>
        public static IEnumerable<IndexInfo> GetTableIndexes(JET_SESID sesid, JET_TABLEID tableid)
        {
            return new GenericEnumerable<IndexInfo>(() => new TableidIndexInfoEnumerator(sesid, tableid));
        }

        /// <summary>
        /// Iterates over all the indexs in the table, returning information about each one.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database containing the table.</param>
        /// <param name="tablename">The name of the table.</param>
        /// <returns>An iterator over an IndexInfo for each index in the table.</returns>
        public static IEnumerable<IndexInfo> GetTableIndexes(JET_SESID sesid, JET_DBID dbid, string tablename)
        {
            if (null == tablename)
            {
                throw new ArgumentNullException("tablename");
            }

            return new GenericEnumerable<IndexInfo>(() => new TableIndexInfoEnumerator(sesid, dbid, tablename));
        }

        /// <summary>
        /// Returns the names of the tables in the database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database containing the table.</param>
        /// <returns>An iterator over the names of the tables in the database.</returns>
        public static IEnumerable<string> GetTableNames(JET_SESID sesid, JET_DBID dbid)
        {
            return new GenericEnumerable<string>(() => new TableNameEnumerator(sesid, dbid));
        }
    }
}