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
    using System.Globalization;
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
                            var columnidValue =
                                (uint)RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcolumnid);

                            var columnid = new JET_COLUMNID() { Value = columnidValue };
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
            JET_COLUMNLIST columnlist;
            Api.JetGetTableColumnInfo(sesid, tableid, string.Empty, out columnlist);
            return EnumerateColumnInfos(sesid, columnlist);
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
            JET_COLUMNLIST columnlist;
            JetGetColumnInfo(sesid, dbid, tablename, string.Empty, out columnlist);
            return EnumerateColumnInfos(sesid, columnlist);
        }

        /// <summary>
        /// Iterates over all the indexes in the table, returning information about each one.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to retrieve index information for.</param>
        /// <returns>An iterator over an IndexInfo for each index in the table.</returns>
        public static IEnumerable<IndexInfo> GetTableIndexes(JET_SESID sesid, JET_TABLEID tableid)
        {
            JET_INDEXLIST indexlist;
            JetGetTableIndexInfo(sesid, tableid, string.Empty, out indexlist);
            return EnumerateIndexInfos(sesid, indexlist);
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
            JET_INDEXLIST indexlist;
            JetGetIndexInfo(sesid, dbid, tablename, string.Empty, out indexlist);
            return EnumerateIndexInfos(sesid, indexlist);
        }

        /// <summary>
        /// Returns the names of the tables in the database.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="dbid">The database containing the table.</param>
        /// <returns>An iterator over the names of the tables in the database.</returns>
        public static IEnumerable<string> GetTableNames(JET_SESID sesid, JET_DBID dbid)
        {
            JET_OBJECTLIST objectlist;
            JetGetObjectInfo(sesid, dbid, out objectlist);
            try
            {
                if (TryMoveFirst(sesid, objectlist.tableid))
                {
                    do
                    {
                        var flags = (uint)RetrieveColumnAsUInt32(sesid, objectlist.tableid, objectlist.columnidflags);
                        if (ObjectInfoFlags.System != ((ObjectInfoFlags)flags & ObjectInfoFlags.System))
                        {
                            yield return
                                RetrieveColumnAsString(
                                    sesid,
                                    objectlist.tableid,
                                    objectlist.columnidobjectname,
                                    NativeMethods.Encoding,
                                    RetrieveColumnGrbit.None);
                        }
                    }
                    while (TryMoveNext(sesid, objectlist.tableid));
                }
            }
            finally
            {
                // Close the temporary table used to return the results
                JetCloseTable(sesid, objectlist.tableid);
            }
        }

        /// <summary>
        /// Iterates over the information in the JET_INDEXLIST, returning information about each index.
        /// The table in the indexlist is closed when finished.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="indexlist">The indexlist to iterate over.</param>
        /// <returns>An iterator over IndexInfo for each index described in the JET_INDEXLIST.</returns>
        private static IEnumerable<IndexInfo> EnumerateIndexInfos(JET_SESID sesid, JET_INDEXLIST indexlist)
        {
            try
            {
                if (Api.TryMoveFirst(sesid, indexlist.tableid))
                {
                    do
                    {
                        yield return GetIndexInfoFromIndexlist(sesid, indexlist);
                    }
                    while (Api.TryMoveNext(sesid, indexlist.tableid));
                }
            }
            finally
            {
                // Close the temporary table used to return the results
                JetCloseTable(sesid, indexlist.tableid);
            }
        }

        /// <summary>
        /// Create an IndexInfo object from the data in the current JET_INDEXLIST entry.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="indexlist">The indexlist to take the data from.</param>
        /// <returns>An IndexInfo object containing the information from that record.</returns>
        private static IndexInfo GetIndexInfoFromIndexlist(JET_SESID sesid, JET_INDEXLIST indexlist)
        {
            string name = RetrieveColumnAsString(
                sesid, indexlist.tableid, indexlist.columnidindexname, NativeMethods.Encoding, RetrieveColumnGrbit.None);
            var lcid = (int) RetrieveColumnAsInt16(sesid, indexlist.tableid, indexlist.columnidLangid);
            var cultureInfo = new CultureInfo(lcid);
            var lcmapFlags = (uint) RetrieveColumnAsUInt32(sesid, indexlist.tableid, indexlist.columnidLCMapFlags);
            CompareOptions compareOptions = Conversions.CompareOptionsFromLCMapFlags(lcmapFlags);
            var grbit = (uint) RetrieveColumnAsUInt32(sesid, indexlist.tableid, indexlist.columnidgrbitIndex);

            IndexSegment[] segments = GetIndexSegmentsFromIndexlist(sesid, indexlist);

            return new IndexInfo(
                name,
                cultureInfo,
                compareOptions,
                segments,
                (CreateIndexGrbit) grbit);
        }

        /// <summary>
        /// Create an array of IndexSegment objects from the data in the current JET_INDEXLIST entry.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="indexlist">The indexlist to take the data from.</param>
        /// <returns>An array of IndexSegment objects containing the information for the current index.</returns>
        private static IndexSegment[] GetIndexSegmentsFromIndexlist(JET_SESID sesid, JET_INDEXLIST indexlist)
        {
            var numSegments = (int) RetrieveColumnAsInt32(sesid, indexlist.tableid, indexlist.columnidcColumn);
            Debug.Assert(numSegments > 0, "Index has zero index segments");

            var segments = new IndexSegment[numSegments];
            for (int i = 0; i < numSegments; ++i)
            {
                string columnName = RetrieveColumnAsString(
                    sesid,
                    indexlist.tableid,
                    indexlist.columnidcolumnname,
                    NativeMethods.Encoding,
                    RetrieveColumnGrbit.None);
                var coltyp = (JET_coltyp) RetrieveColumnAsInt32(sesid, indexlist.tableid, indexlist.columnidcoltyp);
                var grbit =
                    (IndexKeyGrbit) RetrieveColumnAsInt32(sesid, indexlist.tableid, indexlist.columnidgrbitColumn);
                bool isAscending = IndexKeyGrbit.Ascending == grbit;
                var cp = (JET_CP) RetrieveColumnAsInt16(sesid, indexlist.tableid, indexlist.columnidCp);
                bool isASCII = JET_CP.ASCII == cp;

                segments[i] = new IndexSegment(columnName, coltyp, isAscending, isASCII);

                if (i < numSegments - 1)
                {
                    Api.JetMove(sesid, indexlist.tableid, JET_Move.Next, MoveGrbit.None);
                }
            }

            return segments;
        }

        /// <summary>
        /// Iterates over the information in the JET_COLUMNLIST, returning information about each column.
        /// The table in the columnlist is closed when finished.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="columnlist">The columnlist to iterate over.</param>
        /// <returns>An iterator over ColumnInfo for each column described in the JET_COLUMNLIST.</returns>
        private static IEnumerable<ColumnInfo> EnumerateColumnInfos(JET_SESID sesid, JET_COLUMNLIST columnlist)
        {
            try
            {
                if (Api.TryMoveFirst(sesid, columnlist.tableid))
                {
                    do
                    {
                        yield return GetColumnInfoFromColumnlist(sesid, columnlist);
                    }
                    while (Api.TryMoveNext(sesid, columnlist.tableid));
                }
            }
            finally
            {
                // Close the temporary table used to return the results
                JetCloseTable(sesid, columnlist.tableid);
            }
        }

        /// <summary>
        /// Create a ColumnInfo object from the data in the current JET_COLUMNLIST
        /// entry.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="columnlist">The columnlist to take the data from.</param>
        /// <returns>A ColumnInfo object containing the information from that record.</returns>
        private static ColumnInfo GetColumnInfoFromColumnlist(JET_SESID sesid, JET_COLUMNLIST columnlist)
        {
            string name = RetrieveColumnAsString(
                sesid,
                columnlist.tableid,
                columnlist.columnidcolumnname,
                NativeMethods.Encoding,
                RetrieveColumnGrbit.None);
            var columnidValue = (uint) RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcolumnid);
            var coltypValue = (uint) RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcoltyp);
            uint codepageValue = (ushort) RetrieveColumnAsUInt16(sesid, columnlist.tableid, columnlist.columnidCp);
            var maxLength = (uint) RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidcbMax);
            byte[] defaultValue = RetrieveColumn(sesid, columnlist.tableid, columnlist.columnidDefault);
            var grbitValue = (uint) RetrieveColumnAsUInt32(sesid, columnlist.tableid, columnlist.columnidgrbit);

            return new ColumnInfo(
                name,
                new JET_COLUMNID() { Value = columnidValue },
                (JET_coltyp) coltypValue,
                (JET_CP) codepageValue,
                checked((int) maxLength),
                defaultValue,
                (ColumndefGrbit) grbitValue);
        }
    }
}
