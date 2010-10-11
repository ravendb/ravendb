//-----------------------------------------------------------------------
// <copyright file="RetrieveColumnHelpers.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Text;

    /// <summary>
    /// Helper methods for the ESENT API. These aren't interop versions
    /// of the API, but encapsulate very common uses of the functions.
    /// </summary>
    public static partial class Api
    {
        /// <summary>
        /// Cached retrieve buffers.
        /// </summary>
        private static readonly MemoryCache memoryCache = new MemoryCache(128 * 1024, 16);

        #region Nested type: ConversionFunc

        /// <summary>
        /// Conversion function delegate.
        /// </summary>
        /// <typeparam name="TResult">The return type.</typeparam>
        /// <param name="data">The data to convert.</param>
        /// <returns>An object of type TRresult.</returns>
        /// <remarks>
        /// We create this delegate here, instead of using the built-in
        /// Func/Converter generics to avoid taking a dependency on 
        /// a higher version (3.5) of the CLR.
        /// </remarks>
        private delegate TResult ConversionFunc<TResult>(byte[] data);

        #endregion

        /// <summary>
        /// Retrieves the bookmark for the record that is associated with the index entry
        /// at the current position of a cursor. This bookmark can then be used to
        /// reposition that cursor back to the same record using JetGotoBookmark. 
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the bookmark from.</param>
        /// <returns>The bookmark of the record.</returns>
        public static byte[] GetBookmark(JET_SESID sesid, JET_TABLEID tableid)
        {
            // Get the size of the bookmark, allocate memory, retrieve the bookmark.
            int bookmarkSize;
            var err = (JET_err) Impl.JetGetBookmark(sesid, tableid, null, 0, out bookmarkSize);
            if (JET_err.BufferTooSmall != err)
            {
                Check((int) err);
            }

            var bookmark = new byte[bookmarkSize];
            JetGetBookmark(sesid, tableid, bookmark, bookmark.Length, out bookmarkSize);

            return bookmark;
        }

        /// <summary>
        /// Retrieves the key for the index entry at the current position of a cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the key from.</param>
        /// <param name="grbit">Retrieve key options.</param>
        /// <returns>The retrieved key.</returns>
        public static byte[] RetrieveKey(JET_SESID sesid, JET_TABLEID tableid, RetrieveKeyGrbit grbit)
        {
            // Get the size of the key, allocate memory, retrieve the key.
            int keySize;
            JetRetrieveKey(sesid, tableid, null, 0, out keySize, grbit);

            var key = new byte[keySize];
            JetRetrieveKey(sesid, tableid, key, key.Length, out keySize, grbit);

            return key;
        }

        /// <summary>
        /// Retrieves the size of a single column value from the current record.
        /// The record is that record associated with the index entry at the
        /// current position of the cursor. Alternatively, this function can
        /// retrieve a column from a record being created in the cursor copy
        /// buffer. This function can also retrieve column data from an index
        /// entry that references the current record.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The size of the column. 0 if the column is null.</returns>
        public static int? RetrieveColumnSize(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnSize(sesid, tableid, columnid, 1, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves the size of a single column value from the current record.
        /// The record is that record associated with the index entry at the
        /// current position of the cursor. Alternatively, this function can
        /// retrieve a column from a record being created in the cursor copy
        /// buffer. This function can also retrieve column data from an index
        /// entry that references the current record.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="itagSequence">
        /// The sequence number of value in a multi-valued column.
        /// The array of values is one-based. The first value is
        /// sequence 1, not 0. If the record column has only one value then
        /// 1 should be passed as the itagSequence.
        /// </param>
        /// <param name="grbit">Retrieve column options.</param>
        /// <returns>The size of the column. 0 if the column is null.</returns>
        public static int? RetrieveColumnSize(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, int itagSequence, RetrieveColumnGrbit grbit)
        {
            var retinfo = new JET_RETINFO { itagSequence = itagSequence };
            int dataSize;
            JET_wrn wrn = JetRetrieveColumn(
                sesid, tableid, columnid, null, 0, out dataSize, grbit, retinfo);
            if (JET_wrn.ColumnNull == wrn)
            {
                return null;
            }

            return dataSize;
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// Alternatively, this function can retrieve a column from a record being created
        /// in the cursor copy buffer. This function can also retrieve column data from an
        /// index entry that references the current record. In addition to retrieving the
        /// actual column value, JetRetrieveColumn can also be used to retrieve the size
        /// of a column, before retrieving the column data itself so that application
        /// buffers can be sized appropriately.  
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieve column options.</param>
        /// <param name="retinfo">
        /// If pretinfo is give as NULL then the function behaves as though an itagSequence
        /// of 1 and an ibLongValue of 0 (zero) were given. This causes column retrieval to
        /// retrieve the first value of a multi-valued column, and to retrieve long data at
        /// offset 0 (zero).
        /// </param>
        /// <returns>The data retrieved from the column. Null if the column is null.</returns>
        public static byte[] RetrieveColumn(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit, JET_RETINFO retinfo)
        {
            // We expect most column values retrieved this way to be small (retrieving a 1GB LV as one
            // chunk is a bit extreme!). Allocate a small buffer and use that, allocating a larger one
            // if needed.
            var data = new byte[256];
            int dataSize;
            JET_wrn wrn = JetRetrieveColumn(
                sesid, tableid, columnid, data, data.Length, out dataSize, grbit, retinfo);

            if (JET_wrn.ColumnNull == wrn)
            {
                // null column
                data = null;
            }
            else
            {
                Array.Resize(ref data, dataSize);
                if (JET_wrn.BufferTruncated == wrn)
                {
                    // there is more data to retrieve
                    wrn = JetRetrieveColumn(
                        sesid, tableid, columnid, data, data.Length, out dataSize, grbit, retinfo);
                    if (JET_wrn.Success != wrn)
                    {
                        string error = String.Format(
                            CultureInfo.CurrentCulture,
                            "Column size changed from {0} to {1}. The record was probably updated by another thread.",
                            data.Length,
                            dataSize);
                        Trace.TraceError(error);
                        throw new InvalidOperationException(error);
                    }
                }
            }

            return data;
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column. Null if the column is null.</returns>
        public static byte[] RetrieveColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumn(sesid, tableid, columnid, RetrieveColumnGrbit.None, null);
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// The Unicode encoding is used.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a string. Null if the column is null.</returns>
        public static string RetrieveColumnAsString(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsString(sesid, tableid, columnid, Encoding.Unicode, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a string column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="encoding">The string encoding to use when converting data.</param>
        /// <returns>The data retrieved from the column as a string. Null if the column is null.</returns>
        public static string RetrieveColumnAsString(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, Encoding encoding)
        {
            return RetrieveColumnAsString(sesid, tableid, columnid, encoding, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a string column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="encoding">The string encoding to use when converting data.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a string. Null if the column is null.</returns>
        public static string RetrieveColumnAsString(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, Encoding encoding, RetrieveColumnGrbit grbit)
        {
            JET_wrn wrn;

            // This is an optimization for retrieving small Unicode strings
            // We use a stack-allocated buffer to retrieve the data and then create
            // a string from the data
            if (Encoding.Unicode == encoding)
            {
                unsafe
                {
                    // 512 bytes give 256 Unicode characters.
                    const int BufferSize = 512;
                    char* buffer = stackalloc char[BufferSize];
                    int actualDataSize;
                    wrn = JetRetrieveColumn(
                        sesid, tableid, columnid, new IntPtr(buffer), BufferSize, out actualDataSize, grbit, null);
                    if (JET_wrn.ColumnNull == wrn)
                    {
                        return null;
                    }

                    if (JET_wrn.Success == wrn)
                    {
                        return new string(buffer, 0, actualDataSize / sizeof(char));
                    }

                    Debug.Assert(JET_wrn.BufferTruncated == wrn, "Unexpected warning code");

                    // Fallthrough to normal case below
                }
            }

            // Retrieving a string happens in two stages: first the data is retrieved into a data
            // buffer and then the buffer is converted to a string. The buffer isn't needed for
            // very long so we try to use a cached buffer.
            byte[] cachedBuffer = memoryCache.Allocate();
            byte[] data = cachedBuffer;

            int dataSize;
            wrn = JetRetrieveColumn(sesid, tableid, columnid, data, data.Length, out dataSize, grbit, null);
            if (JET_wrn.ColumnNull == wrn)
            {
                return null;
            }

            if (JET_wrn.BufferTruncated == wrn)
            {
                Debug.Assert(dataSize > data.Length, "Expected column to be bigger than buffer");
                data = new byte[dataSize];
                wrn = JetRetrieveColumn(sesid, tableid, columnid, data, data.Length, out dataSize, grbit, null);
                if (JET_wrn.BufferTruncated == wrn)
                {
                    string error = String.Format(
                        CultureInfo.CurrentCulture,
                        "Column size changed from {0} to {1}. The record was probably updated by another thread.",
                        data.Length,
                        dataSize);
                    Trace.TraceError(error);
                    throw new InvalidOperationException(error);
                }
            }

            // TODO: for Unicode strings pin the buffer and use the String(char*) constructor.
            string s = encoding.GetString(data, 0, dataSize);

            // Now we have extracted the string from the buffer we can free (cache) the buffer.
            memoryCache.Free(cachedBuffer);

            return s;
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a short. Null if the column is null.</returns>
        public static short? RetrieveColumnAsInt16(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsInt16(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves an int16 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a short. Null if the column is null.</returns>
        public static short? RetrieveColumnAsInt16(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(short);
                short data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as an int. Null if the column is null.</returns>
        public static int? RetrieveColumnAsInt32(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsInt32(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves an int32 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as an int. Null if the column is null.</returns>
        public static int? RetrieveColumnAsInt32(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(int);
                int data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a long. Null if the column is null.</returns>
        public static long? RetrieveColumnAsInt64(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsInt64(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a single column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a long. Null if the column is null.</returns>
        public static long? RetrieveColumnAsInt64(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(long);
                long data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a float column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a float. Null if the column is null.</returns>
        public static float? RetrieveColumnAsFloat(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsFloat(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a float column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a float. Null if the column is null.</returns>
        public static float? RetrieveColumnAsFloat(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(float);
                float data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a double column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a double. Null if the column is null.</returns>
        public static double? RetrieveColumnAsDouble(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsDouble(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a double column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a double. Null if the column is null.</returns>
        public static double? RetrieveColumnAsDouble(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(double);
                double data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a boolean column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a boolean. Null if the column is null.</returns>
        public static bool? RetrieveColumnAsBoolean(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsBoolean(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a boolean column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a boolean. Null if the column is null.</returns>
        public static bool? RetrieveColumnAsBoolean(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            byte? b = RetrieveColumnAsByte(sesid, tableid, columnid, grbit);
            if (b.HasValue)
            {
                return 0 != b.Value;
            }

            return new bool?();
        }

        /// <summary>
        /// Retrieves a byte column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a byte. Null if the column is null.</returns>
        public static byte? RetrieveColumnAsByte(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsByte(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a byte column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a byte. Null if the column is null.</returns>
        public static byte? RetrieveColumnAsByte(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(byte);
                byte data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a guid column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a guid. Null if the column is null.</returns>
        public static Guid? RetrieveColumnAsGuid(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsGuid(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a guid column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a guid. Null if the column is null.</returns>
        public static Guid? RetrieveColumnAsGuid(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = 16;
                Guid data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a datetime column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as a datetime. Null if the column is null.</returns>
        public static DateTime? RetrieveColumnAsDateTime(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsDateTime(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a datetime column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as a datetime. Null if the column is null.</returns>
        public static DateTime? RetrieveColumnAsDateTime(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            // Internally DateTime is stored in OLE Automation format
            double? oadate = RetrieveColumnAsDouble(sesid, tableid, columnid, grbit);
            if (oadate.HasValue)
            {
                return Conversions.ConvertDoubleToDateTime(oadate.Value);
            }

            return new DateTime?();
        }

        /// <summary>
        /// Retrieves a uint16 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as an UInt16. Null if the column is null.</returns>
        [CLSCompliant(false)]
        public static ushort? RetrieveColumnAsUInt16(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsUInt16(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a uint16 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as an UInt16. Null if the column is null.</returns>
        [CLSCompliant(false)]
        public static ushort? RetrieveColumnAsUInt16(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(ushort);
                ushort data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a uint32 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as an UInt32. Null if the column is null.</returns>
        [CLSCompliant(false)]
        public static uint? RetrieveColumnAsUInt32(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsUInt32(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a uint32 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as an UInt32. Null if the column is null.</returns>
        [CLSCompliant(false)]
        public static uint? RetrieveColumnAsUInt32(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(uint);
                uint data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Retrieves a uint64 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <returns>The data retrieved from the column as an UInt64. Null if the column is null.</returns>
        [CLSCompliant(false)]
        public static ulong? RetrieveColumnAsUInt64(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return RetrieveColumnAsUInt64(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Retrieves a uint64 column value from the current record. The record is that
        /// record associated with the index entry at the current position of the cursor.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="grbit">Retrieval options.</param>
        /// <returns>The data retrieved from the column as an UInt64. Null if the column is null.</returns>
        [CLSCompliant(false)]
        public static ulong? RetrieveColumnAsUInt64(
            JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            unsafe
            {
                const int DataSize = sizeof(ulong);
                ulong data;
                var pointer = new IntPtr(&data);
                int actualDataSize;
                JET_wrn wrn = JetRetrieveColumn(
                    sesid, tableid, columnid, pointer, DataSize, out actualDataSize, grbit, null);
                return CreateReturnValue(data, DataSize, wrn, actualDataSize);
            }
        }

        /// <summary>
        /// Deserialize an object from a column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to read from.</param>
        /// <param name="columnid">The column to read from.</param>
        /// <returns>The deserialized object. Null if the column was null.</returns>
        public static object DeserializeObjectFromColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            return DeserializeObjectFromColumn(sesid, tableid, columnid, RetrieveColumnGrbit.None);
        }

        /// <summary>
        /// Deserialize an object from a column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to read from.</param>
        /// <param name="columnid">The column to read from.</param>
        /// <param name="grbit">The retrieval options to use.</param>
        /// <returns>The deserialized object. Null if the column was null.</returns>
        public static object DeserializeObjectFromColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, RetrieveColumnGrbit grbit)
        {
            int actualSize;
            if (JET_wrn.ColumnNull == Api.JetRetrieveColumn(sesid, tableid, columnid, null, 0, out actualSize, grbit, null))
            {
                return null;
            }

            using (var stream = new ColumnStream(sesid, tableid, columnid))
            {
                var deseriaizer = new BinaryFormatter();
                return deseriaizer.Deserialize(stream);
            }
        }

        /// <summary>
        /// Retrieves columns into ColumnValue objects.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor retrieve the data from. The cursor should be positioned on a record.</param>
        /// <param name="values">The values to retrieve.</param>
        public static void RetrieveColumns(JET_SESID sesid, JET_TABLEID tableid, params ColumnValue[] values)
        {
            if (null == values)
            {
                throw new ArgumentNullException("values");
            }

            if (0 == values.Length)
            {
                throw new ArgumentOutOfRangeException("values", values.Length, "must have at least one value");
            }

            Api.Check(ColumnValue.RetrieveColumns(sesid, tableid, values));
        }

        /// <summary>
        /// Create the nullable return value.
        /// </summary>
        /// <typeparam name="T">The (struct) type to return.</typeparam>
        /// <param name="data">The data retrieved from the column.</param>
        /// <param name="dataSize">The size of the data.</param>
        /// <param name="wrn">The warning code from esent.</param>
        /// <param name="actualDataSize">The actual size of the data retireved fomr esent.</param>
        /// <returns>A nullable struct of type T.</returns>
        private static T? CreateReturnValue<T>(T data, int dataSize, JET_wrn wrn, int actualDataSize) where T : struct
        {
            if (JET_wrn.ColumnNull == wrn)
            {
                return new T?();
            }

            CheckDataSize(dataSize, actualDataSize);
            return data;
        }

        /// <summary>
        /// Make sure the retrieved data size is at least as long as the expected size.
        /// An exception is thrown if the data isn't long enough.
        /// </summary>
        /// <param name="expectedDataSize">The expected data size.</param>
        /// <param name="actualDataSize">The size of the retrieved data.</param>
        private static void CheckDataSize(int expectedDataSize, int actualDataSize)
        {
            if (actualDataSize < expectedDataSize)
            {
                throw new EsentInvalidColumnException();
            }
        }

        /// <summary>
        /// Recursively pin the retrieve buffers in the JET_RETRIEVECOLUMN
        /// structures and then retrieve the columns. This is done to avoid
        /// creating GCHandles which is very expensive. This function pins
        /// the current retrievecolumn structure (indicated by i) and then
        /// recursively calls itself until all structures are pinned. This
        /// is done because it isn't possible to create an arbitrary number
        /// of pinned variables in a method.
        /// </summary>
        /// <param name="sesid">
        /// The session to use.
        /// </param>
        /// <param name="tableid">
        /// The table to retrieve from.
        /// </param>
        /// <param name="nativeretrievecolumns">
        /// The nativeretrievecolumns structure.</param>
        /// <param name="retrievecolumns">
        /// The managed retrieve columns structure.
        /// </param>
        /// <param name="numColumns">The number of columns.</param>
        /// <param name="i">The column currently being processed.</param>
        /// <returns>An error code from JetRetrieveColumns.</returns>
        private static unsafe int PinColumnsAndRetrieve(
            JET_SESID sesid,
            JET_TABLEID tableid,
            NATIVE_RETRIEVECOLUMN* nativeretrievecolumns,
            JET_RETRIEVECOLUMN[] retrievecolumns,
            int numColumns,
            int i)
        {
            retrievecolumns[i].CheckDataSize();
            nativeretrievecolumns[i] = retrievecolumns[i].GetNativeRetrievecolumn();
            fixed (byte* pinnedBuffer = retrievecolumns[i].pvData)
            {
                nativeretrievecolumns[i].pvData = new IntPtr(pinnedBuffer);

                if (numColumns - 1 == i)
                {
                    return Impl.JetRetrieveColumns(sesid, tableid, nativeretrievecolumns, numColumns);
                }

                return PinColumnsAndRetrieve(sesid, tableid, nativeretrievecolumns, retrievecolumns, numColumns, i + 1);
            }
        }
    }
}
