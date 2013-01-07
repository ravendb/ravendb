//-----------------------------------------------------------------------
// <copyright file="InternalApi.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Internal-only methods of the Api.
    /// </summary>
    public static partial class Api
    {
        /// <summary>
        /// The JetSetColumn function modifies a single column value in a modified record to be inserted or to
        /// update the current record. It can overwrite an existing value, add a new value to a sequence of
        /// values in a multi-valued column, remove a value from a sequence of values in a multi-valued column,
        /// or update all or part of a long value (a column of type <see cref="JET_coltyp.LongText"/>
        /// or <see cref="JET_coltyp.LongBinary"/>). 
        /// </summary>
        /// <remarks>
        /// This is an internal-only version of the API that takes a data buffer and an offset into the buffer.
        /// </remarks>
        /// <param name="sesid">The session which is performing the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <param name="columnid">The columnid to set.</param>
        /// <param name="data">The data to set.</param>
        /// <param name="dataSize">The size of data to set.</param>
        /// <param name="dataOffset">The offset in the data buffer to set data from.</param>
        /// <param name="grbit">SetColumn options.</param>
        /// <param name="setinfo">Used to specify itag or long-value offset.</param>
        /// <returns>A warning value.</returns>
        public static JET_wrn JetSetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, byte[] data, int dataSize, int dataOffset, SetColumnGrbit grbit, JET_SETINFO setinfo)
        {
            if (dataOffset < 0
                || (null != data && 0 != dataSize && dataOffset >= data.Length)
                || (null == data && dataOffset != 0))
            {
                throw new ArgumentOutOfRangeException(
                    "dataOffset",
                    dataOffset,
                    "must be inside the data buffer");                    
            }

            if (null != data && dataSize > checked(data.Length - dataOffset) && (SetColumnGrbit.SizeLV != (grbit & SetColumnGrbit.SizeLV)))
            {
                throw new ArgumentOutOfRangeException(
                    "dataSize",
                    dataSize,
                    "cannot be greater than the length of the data (unless the SizeLV option is used)");
            }

            unsafe
            {
                fixed (byte* pointer = data)
                {
                    return Api.JetSetColumn(sesid, tableid, columnid, new IntPtr(pointer + dataOffset), dataSize, grbit, setinfo);
                }
            }
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
        /// <remarks>
        /// This is an internal method that takes a buffer offset as well as size.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="data">The data buffer to be retrieved into.</param>
        /// <param name="dataSize">The size of the data buffer.</param>
        /// <param name="dataOffset">Offset into the data buffer to read data into.</param>
        /// <param name="actualDataSize">Returns the actual size of the data buffer.</param>
        /// <param name="grbit">Retrieve column options.</param>
        /// <param name="retinfo">
        /// If pretinfo is give as NULL then the function behaves as though an itagSequence
        /// of 1 and an ibLongValue of 0 (zero) were given. This causes column retrieval to
        /// retrieve the first value of a multi-valued column, and to retrieve long data at
        /// offset 0 (zero).
        /// </param>
        /// <returns>An ESENT warning code.</returns>
        public static JET_wrn JetRetrieveColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, byte[] data, int dataSize, int dataOffset, out int actualDataSize, RetrieveColumnGrbit grbit, JET_RETINFO retinfo)
        {
            if (dataOffset < 0
                || (null != data && 0 != dataSize && dataOffset >= data.Length)
                || (null == data && dataOffset != 0))
            {
                throw new ArgumentOutOfRangeException(
                    "dataOffset",
                    dataOffset,
                    "must be inside the data buffer");
            }

            if ((null == data && dataSize > 0) || (null != data && dataSize > data.Length))
            {
                throw new ArgumentOutOfRangeException(
                    "dataSize",
                    dataSize,
                    "cannot be greater than the length of the data");
            }

            unsafe
            {
                fixed (byte* pointer = data)
                {
                    return Api.JetRetrieveColumn(
                        sesid, tableid, columnid, new IntPtr(pointer + dataOffset), dataSize, out actualDataSize, grbit, retinfo);
                }
            }
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
        /// <remarks>
        /// This is an internal-use version that takes an IntPtr.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to retrieve the column from.</param>
        /// <param name="columnid">The columnid to retrieve.</param>
        /// <param name="data">The data buffer to be retrieved into.</param>
        /// <param name="dataSize">The size of the data buffer.</param>
        /// <param name="actualDataSize">Returns the actual size of the data buffer.</param>
        /// <param name="grbit">Retrieve column options.</param>
        /// <param name="retinfo">
        /// If pretinfo is give as NULL then the function behaves as though an itagSequence
        /// of 1 and an ibLongValue of 0 (zero) were given. This causes column retrieval to
        /// retrieve the first value of a multi-valued column, and to retrieve long data at
        /// offset 0 (zero).
        /// </param>
        /// <returns>An ESENT warning code.</returns>
        internal static JET_wrn JetRetrieveColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, IntPtr data, int dataSize, out int actualDataSize, RetrieveColumnGrbit grbit, JET_RETINFO retinfo)
        {
            return Api.Check(Impl.JetRetrieveColumn(sesid, tableid, columnid, data, dataSize, out actualDataSize, grbit, retinfo));
        }

        /// <summary>
        /// Constructs search keys that may then be used by JetSeek and JetSetIndexRange.
        /// </summary>
        /// <remarks>
        /// This is an internal (unsafe) version that takes an IntPtr.
        /// </remarks>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor to create the key on.</param>
        /// <param name="data">Column data for the current key column of the current index.</param>
        /// <param name="dataSize">Size of the data.</param>
        /// <param name="grbit">Key options.</param>
        internal static void JetMakeKey(JET_SESID sesid, JET_TABLEID tableid, IntPtr data, int dataSize, MakeKeyGrbit grbit)
        {
            Api.Check(Impl.JetMakeKey(sesid, tableid, data, dataSize, grbit));
        }

        /// <summary>
        /// The JetSetColumn function modifies a single column value in a modified record to be inserted or to
        /// update the current record. It can overwrite an existing value, add a new value to a sequence of
        /// values in a multi-valued column, remove a value from a sequence of values in a multi-valued column,
        /// or update all or part of a long value, a column of type JET_coltyp.LongText or JET_coltyp.LongBinary. 
        /// </summary>
        /// <remarks>
        /// This method takes an IntPtr and is intended for internal use only.
        /// </remarks>
        /// <param name="sesid">The session which is performing the update.</param>
        /// <param name="tableid">The cursor to update. An update should be prepared.</param>
        /// <param name="columnid">The columnid to set.</param>
        /// <param name="data">The data to set.</param>
        /// <param name="dataSize">The size of data to set.</param>
        /// <param name="grbit">SetColumn options.</param>
        /// <param name="setinfo">Used to specify itag or long-value offset.</param>
        /// <returns>A warning value.</returns>
        internal static JET_wrn JetSetColumn(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, IntPtr data, int dataSize, SetColumnGrbit grbit, JET_SETINFO setinfo)
        {
            return Api.Check(Impl.JetSetColumn(sesid, tableid, columnid, data, dataSize, grbit, setinfo));
        }
    }
}